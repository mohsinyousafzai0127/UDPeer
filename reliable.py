"""reliable.py - reliable delivery on top of UDP. Owner: Mohsin.

Rules :
 - seq numbers are per (sender -> receiver) pair, starting at 1.
 - Reliable message: send, wait for ACK, retransmit after 0.5s (max ~10 tries).
 - Receiver: always ACK, drop duplicates, hold early packets, deliver in order.
"""
import protocol


class ReliableChannel:
    def __init__(self, my_id, port, on_message):
        """on_message(msg_type, sender_id, payload, addr) is called for every
        delivered message (Shahab's code provides it)."""
        self.my_id = my_id
        self.sock = protocol.UdpSocket(port)
        self.on_message = on_message

    def start(self):
        """Start the receive thread."""
        pass  # TODO Day 3

    def stop(self):
        pass  # TODO

    def send_reliable(self, addr, msg_type, payload=b""):
        """Send and retry until ACKed. Returns True if ACKed, False if gave up."""
        pass  # TODO Day 3

    def send_unreliable(self, addr, msg_type, payload=b""):
        """Fire and forget (heartbeats)."""
        self.sock.send(protocol.pack(msg_type, self.my_id, payload), addr)
