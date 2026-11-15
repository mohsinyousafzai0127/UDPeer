"""peers.py - peer table, join, heartbeat. Owner: Shahab."""
import threading


class PeerTable:
    def __init__(self):
        self.lock = threading.Lock()
        self.peers = {}  # peer_id -> {"addr": (ip, port), "last_seen": time}

    def add(self, peer_id, addr):
        pass  # TODO Day 2

    def remove(self, peer_id):
        pass  # TODO Day 3

    def get_peers(self):
        """Return {peer_id: (ip, port)} of all alive peers."""
        pass  # TODO Day 2


def join(channel, table, bootstrap_addr):
    """Send JOIN to the bootstrap peer, fill the table from WELCOME."""
    pass  # TODO Day 2


def start_heartbeat(channel, table):
    """Thread: send HEARTBEAT every 1s, remove peers silent for 5s."""
    pass  # TODO Day 3
