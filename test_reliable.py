"""Phase 1 test.  Run: python test_reliable.py"""
import time

import protocol
from reliable import ReliableChannel

got = []   # what peer B receives


def on_b(msg_type, sender, payload, addr):
    if msg_type == protocol.CHAT:
        got.append(int(payload.decode()))


def on_a(msg_type, sender, payload, addr):
    pass


a = ReliableChannel(1, 6101, on_a)
b = ReliableChannel(2, 6102, on_b)
a.start(); b.start()
B = ("127.0.0.1", 6102)

# the first packet to a peer must be a handshake (JOIN); it starts the session
assert a.send_reliable(B, protocol.JOIN, b"{}")

protocol.LOSS = 0.3   # drop 30% of ALL packets (data and ACKs)
for i in range(100):
    a.send_async(B, protocol.CHAT, str(i).encode())
assert a.send_many(B, protocol.CHAT, [str(i).encode() for i in range(100, 300)])
time.sleep(2)
protocol.LOSS = 0.0

assert got == list(range(300)), f"wrong order / lost / duplicated: {len(got)} received"
print(f"300 messages delivered in order, no duplicates, no loss "
      f"(sent {a.stats['sent']} packets, {a.stats['retransmits']} retransmissions)")
a.stop(); b.stop()
print("Phase 1 OK")
