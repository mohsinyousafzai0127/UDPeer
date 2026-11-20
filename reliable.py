"""reliable.py - TCP-like reliability on top of UDP.  Owner: Mohsin.

How it works (a small version of TCP):
 * Every reliable packet carries a sequence number (seq). The receiver answers
   every packet with an ACK (one ACK per packet = "selective ACK").
 * The sender keeps at most WINDOW un-ACKed packets in flight (sliding window)
   and re-sends a packet when its ACK does not arrive within RTO seconds.
 * The receiver drops duplicates and delivers packets IN ORDER per sender
   (packets that arrive early wait in a buffer).
 * seq numbers are per (sender -> receiver) pair and start at a random number.
   JOIN / WELCOME packets start a new session, so a peer that restarts is accepted.
 * No congestion control (the manual says we can ignore it).
"""
import queue
import random
import threading
import time

import protocol

RTO = 0.2            # retransmit timeout (seconds)
MAX_TRIES = 30       # give up on a packet after this many sends
WINDOW = 32          # max packets in flight
MAX_BUFFER = 5000    # how far ahead the receiver accepts packets
PACKET_DELAY = 0.0   # sleep before each data packet (simulates a slow uplink / slow motion)

HANDSHAKE = (protocol.JOIN, protocol.WELCOME)


class ReliableChannel:
    def __init__(self, my_id, port, on_message):
        """on_message(msg_type, sender_id, payload, addr) is called for every
        message that is delivered (reliable ones: once, in order)."""
        self.my_id = my_id
        self.on_message = on_message
        self.sock = protocol.UdpSocket(port)
        self.running = False

        self.seq_lock = threading.Lock()
        self.send_seq = {}                       # addr -> next seq to use
        self.base = random.randint(1, 1 << 30)   # first seq for every new destination

        self.cond = threading.Condition()        # protects 'acked'
        self.acked = set()                       # {(addr, seq)}

        self.recv_state = {}                     # sender_id -> {"expected", "buffer", "hs_seq"}
        self.deliver_queue = queue.Queue()
        self.pace_lock = threading.Lock()
        self.stats = {"sent": 0, "retransmits": 0}

    # ------------------------------------------------------------ start/stop
    def start(self):
        self.running = True
        self.rx_thread = threading.Thread(target=self._receive_loop, daemon=True)
        self.rx_thread.start()
        threading.Thread(target=self._deliver_loop, daemon=True).start()

    def stop(self):
        self.running = False
        if threading.current_thread() is not self.rx_thread:
            self.rx_thread.join(1.0)       # let the receive loop finish before closing the socket
        self.sock.close()

    # ------------------------------------------------------------ sending
    def send_unreliable(self, addr, msg_type, payload=b""):
        """Fire and forget (heartbeats)."""
        self.sock.send(protocol.pack(msg_type, self.my_id, payload), addr)

    def send_reliable(self, addr, msg_type, payload=b"", max_tries=MAX_TRIES):
        """Send one message and block until it is ACKed. True = delivered."""
        return self.send_many(addr, msg_type, [payload], max_tries=max_tries)

    def send_many(self, addr, msg_type, payloads, window=WINDOW, max_tries=MAX_TRIES):
        """Send a list of payloads with a sliding window. True when all are ACKed."""
        seqs = self._next_seqs(addr, len(payloads))
        return self._transmit(addr, msg_type, payloads, seqs, window, max_tries)

    def send_async(self, addr, msg_type, payload=b""):
        """Reliable send in a background thread. The seq number is taken NOW,
        so messages keep the order in which send_async was called."""
        seqs = self._next_seqs(addr, 1)
        threading.Thread(target=self._transmit, args=(addr, msg_type, [payload], seqs),
                         daemon=True).start()

    def _next_seqs(self, addr, n):
        with self.seq_lock:
            first = self.send_seq.get(addr, self.base)
            self.send_seq[addr] = first + n
        return list(range(first, first + n))

    def _transmit(self, addr, msg_type, payloads, seqs, window=WINDOW, max_tries=MAX_TRIES):
        packets = {s: protocol.pack(msg_type, self.my_id, p, seq=s, flags=protocol.FLAG_RELIABLE)
                   for s, p in zip(seqs, payloads)}
        last_sent, tries = {}, {}
        while packets:
            if not self.running:
                return False
            now = time.time()
            for s in sorted(packets)[:window]:               # the 'window'
                if s in last_sent and now - last_sent[s] < RTO:
                    continue                                 # still waiting for its ACK
                if tries.get(s, 0) >= max_tries:
                    return False                             # peer is gone
                if s in last_sent:
                    self.stats["retransmits"] += 1
                if PACKET_DELAY:
                    with self.pace_lock:
                        time.sleep(PACKET_DELAY)
                self.sock.send(packets[s], addr)
                self.stats["sent"] += 1
                last_sent[s] = time.time()
                tries[s] = tries.get(s, 0) + 1
            with self.cond:                                  # collect ACKs
                done = [s for s in packets if (addr, s) in self.acked]
                if not done:
                    self.cond.wait(0.05)
                    done = [s for s in packets if (addr, s) in self.acked]
                for s in done:
                    self.acked.discard((addr, s))
                    del packets[s]
        return True

    # ------------------------------------------------------------ receiving
    def _receive_loop(self):
        while self.running:
            try:
                msg, addr = self.sock.receive(timeout=0.5)
                if msg is not None:
                    self._handle(msg, addr)
            except Exception as e:
                if self.running:
                    print("[reliable error]", e)

    def _handle(self, msg, addr):
        t, sender, seq = msg["type"], msg["sender"], msg["seq"]

        if t == protocol.ACK:
            with self.cond:
                self.acked.add((addr, msg["ack"]))
                self.cond.notify_all()
            return

        if not (msg["flags"] & protocol.FLAG_RELIABLE):
            self.on_message(t, sender, msg["payload"], addr)   # heartbeat etc.
            return

        state = self.recv_state.get(sender)
        if t in HANDSHAKE:
            if state is None or state["hs_seq"] != seq:        # new session
                state = {"expected": seq, "buffer": {}, "hs_seq": seq}
                self.recv_state[sender] = state
        elif state is None:
            return                          # unknown sender: no ACK, it will retry later
        if seq >= state["expected"] + MAX_BUFFER:
            return
        self.sock.send(protocol.pack(protocol.ACK, self.my_id, ack=seq), addr)
        if seq < state["expected"]:
            return                          # duplicate: ACKed again but not delivered
        state["buffer"][seq] = (msg, addr)
        while state["expected"] in state["buffer"]:            # deliver in order
            self.deliver_queue.put(state["buffer"].pop(state["expected"]))
            state["expected"] += 1

    def _deliver_loop(self):
        while self.running:
            try:
                msg, addr = self.deliver_queue.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                self.on_message(msg["type"], msg["sender"], msg["payload"], addr)
            except Exception as e:
                print("[handler error]", e)
