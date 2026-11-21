"""peers.py - membership: peer table, join, heartbeat, leave.  Owner: Shahab.

Flow:
  new peer N  --JOIN-->  bootstrap B        B adds N and replies WELCOME (+ FILE_LIST per file)
  N reads B's peer list and sends JOIN to every other peer too -> everyone knows everyone.
  After that the bootstrap is not needed any more (full mesh).
  Every peer sends HEARTBEAT every 1 s; a peer silent for DEAD_AFTER seconds is removed.
"""
import threading
import time

import protocol

HEARTBEAT_EVERY = 1.0
DEAD_AFTER = 6.0


class PeerTable:
    def __init__(self):
        self.lock = threading.Lock()
        self.peers = {}        # peer_id -> {"addr": (ip, port), "last_seen": time}
        self.left = set()      # peers that said LEAVE (ignore their late heartbeats)

    def add(self, peer_id, addr):
        """Add or refresh a peer. True if it was not in the table."""
        with self.lock:
            new = peer_id not in self.peers
            self.peers[peer_id] = {"addr": tuple(addr), "last_seen": time.time()}
            self.left.discard(peer_id)
            return new

    def touch(self, peer_id):
        with self.lock:
            if peer_id in self.peers:
                self.peers[peer_id]["last_seen"] = time.time()
                return True
            return False

    def remove(self, peer_id, left=False):
        with self.lock:
            if left:
                self.left.add(peer_id)
            return self.peers.pop(peer_id, None) is not None

    def is_left(self, peer_id):
        with self.lock:
            return peer_id in self.left

    def get_peers(self):
        """{peer_id: (ip, port)} of all alive peers (not including me)."""
        with self.lock:
            return {pid: p["addr"] for pid, p in self.peers.items()}

    def find_dead(self, timeout):
        now = time.time()
        with self.lock:
            return [pid for pid, p in self.peers.items() if now - p["last_seen"] > timeout]


# ------------------------------------------------------------------ join
def join(node, bootstrap_addr):
    """Join the group through a bootstrap peer. True on success."""
    node.welcomed.clear()
    if not node.channel.send_reliable(bootstrap_addr, protocol.JOIN, protocol.pack_json({})):
        print("could not reach the bootstrap peer", bootstrap_addr)
        return False
    if not node.welcomed.wait(10):
        print("bootstrap peer did not answer with WELCOME")
        return False
    # introduce ourselves to everybody else (in parallel)
    threads = []
    for pid, addr in list(node.contacts.items()):
        if pid != node.my_id and pid not in node.table.get_peers():
            t = threading.Thread(target=node.channel.send_reliable,
                                 args=(addr, protocol.JOIN, protocol.pack_json({})), daemon=True)
            t.start()
            threads.append(t)
    for t in threads:
        t.join(10)
    t0 = time.time()                       # wait for the WELCOME answers of the other peers
    while time.time() - t0 < 5 and any(pid not in node.table.get_peers() for pid in node.contacts):
        time.sleep(0.1)
    print(f"joined the group, peers: {sorted(node.table.get_peers())}")
    return True


def handle_join(node, sender, addr):
    """Someone wants to join: answer with WELCOME (peer list) and the file list."""
    others = {str(pid): list(a) for pid, a in node.table.get_peers().items() if pid != sender}
    node.channel.send_reliable(addr, protocol.WELCOME, protocol.pack_json({"peers": others}))
    new = node.table.add(sender, addr)     # add AFTER the WELCOME so chat seq numbers stay ordered
    if new:
        print(f"\n[peer {sender} joined]")
    for m in node.store.manifests():
        node.channel.send_reliable(addr, protocol.FILE_LIST, protocol.pack_json(m))


def handle_welcome(node, sender, addr, payload):
    """Answer to our JOIN: remember the sender and who else is in the group."""
    node.table.add(sender, addr)
    for pid, a in protocol.unpack_json(payload)["peers"].items():
        pid = int(pid)
        if pid != node.my_id and pid != sender:
            node.contacts[pid] = tuple(a)
    node.welcomed.set()


# ------------------------------------------------------------------ heartbeat / failure detector
def handle_heartbeat(node, sender, addr):
    if node.table.is_left(sender):
        return
    if not node.table.touch(sender):
        node.table.add(sender, addr)        # we wrongly declared it dead (lost heartbeats)
        print(f"\n[peer {sender} is back]")


def heartbeat_loop(node):
    """Thread: send HEARTBEAT to everyone every second, drop peers that went silent."""
    while node.running:
        for pid, addr in node.table.get_peers().items():
            node.channel.send_unreliable(addr, protocol.HEARTBEAT)
        for pid in node.table.find_dead(DEAD_AFTER):
            if node.table.remove(pid):
                print(f"\n[peer {pid} timed out - removed]")
        time.sleep(HEARTBEAT_EVERY)


# ------------------------------------------------------------------ leave
def handle_leave(node, sender):
    if node.table.remove(sender, left=True):
        print(f"\n[peer {sender} left]")


def leave(node):
    """Tell everybody we are leaving, then stop."""
    threads = []
    for pid, addr in node.table.get_peers().items():
        t = threading.Thread(target=node.channel.send_reliable,
                             args=(addr, protocol.LEAVE), daemon=True)
        t.start()
        threads.append(t)
    for t in threads:
        t.join(3)
    node.stop()
