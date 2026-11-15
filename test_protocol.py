"""Run: python test_protocol.py"""
import protocol as p

# 1. pack/unpack roundtrip
raw = p.pack(p.CHAT, 7, b"hello", seq=3, flags=p.FLAG_RELIABLE)
msg = p.unpack(raw)
assert msg["type"] == p.CHAT and msg["sender"] == 7
assert msg["seq"] == 3 and msg["payload"] == b"hello"
assert len(raw) == p.HEADER_SIZE + 5

# 2. corrupted packet is rejected
bad = bytearray(raw)
bad[-1] ^= 1
assert p.unpack(bytes(bad)) is None

# 3. chunk helpers
payload = p.pack_chunk_data(1, 2, 3, 64, b"xyz")
assert p.unpack_chunk_data(payload) == (1, 2, 3, 64, b"xyz")

# 4. real UDP on localhost
a, b = p.UdpSocket(6001), p.UdpSocket(6002)
a.send(p.pack(p.HEARTBEAT, 1), ("127.0.0.1", 6002))
m, addr = b.receive(timeout=2)
assert m is not None and m["type"] == p.HEARTBEAT

# 5. loss switch drops everything at LOSS = 1
p.LOSS = 1.0
a.send(p.pack(p.HEARTBEAT, 1), ("127.0.0.1", 6002))
m, addr = b.receive(timeout=0.5)
assert m is None
p.LOSS = 0.0

a.close(); b.close()
print("All protocol tests passed")
