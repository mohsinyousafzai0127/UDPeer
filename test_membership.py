"""Phase 3 test (membership + chat, 4 peers on localhost).  Run: python test_membership.py"""
import shutil
import time

import chat
import peers
import protocol
from node import Node

chat.VERBOSE = False
shutil.rmtree("data_test", ignore_errors=True)
nodes = [Node(i, 6200 + i, base="data_test") for i in range(1, 5)]
for n in nodes:
    n.start()
boot = ("127.0.0.1", 6201)
for n in nodes[1:]:
    assert peers.join(n, boot)
time.sleep(1)
for n in nodes:
    assert sorted(n.table.get_peers()) == [i for i in range(1, 5) if i != n.my_id], n.table.get_peers()
print("every peer knows every other peer (no server needed after joining)")

# chat with 30% packet loss
protocol.LOSS = 0.3
for i in range(40):
    chat.send_chat(nodes[0], f"msg{i}")
    chat.send_chat(nodes[1], f"other{i}")
time.sleep(6)
protocol.LOSS = 0.0
for n in nodes[2:]:
    from1 = [t for s, t, _ in n.chat_log if s == 1]
    from2 = [t for s, t, _ in n.chat_log if s == 2]
    assert from1 == [f"msg{i}" for i in range(40)], from1
    assert from2 == [f"other{i}" for i in range(40)], from2
print("chat: in order, no duplicates, no loss at 30% packet loss")

# a peer leaves politely, another one crashes
peers.leave(nodes[3])
time.sleep(1)
assert 4 not in nodes[0].table.get_peers()
print("LEAVE detected")
t0 = time.time()
nodes[2].stop()                      # crash: no LEAVE message
while 3 in nodes[0].table.get_peers() and time.time() - t0 < 15:
    time.sleep(0.2)
print(f"crash detected after {time.time() - t0:.1f}s")
assert 3 not in nodes[0].table.get_peers() and 3 not in nodes[1].table.get_peers()
for n in nodes:
    n.stop()
shutil.rmtree("data_test", ignore_errors=True)
print("Phase 3 OK")
