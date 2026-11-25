"""Phase 6/7 test: the whole system with real sync (replication, join takeover, re-replication).
Run: python test_system.py   (takes about 40 seconds)"""
import os
import shutil
import threading
import time

import chat
import peers
import reliable
import sync
import transfer
import storage
from node import Node

chat.VERBOSE = False
shutil.rmtree("data_test", ignore_errors=True)
os.makedirs("data_test", exist_ok=True)
files = {}
for name, size in (("a.bin", 2_300_000), ("b.bin", 2_100_000)):
    files[name] = os.urandom(size)
    open(f"data_test/{name}", "wb").write(files[name])


def make(i):
    n = Node(i, 6500 + i, "data_test")
    n.start(); sync.start(n)
    if i > 1:
        assert peers.join(n, ("127.0.0.1", 6501))
    return n


def copies(nodes, m):
    """For every chunk: on how many alive peers' stores is it?"""
    return [sum(1 for n in nodes if n.store.has_chunk(m["file_id"], c))
            for c in range(len(m["chunk_hashes"]))]


def wait_for(cond, timeout, what):
    t0 = time.time()
    while not cond():
        assert time.time() - t0 < timeout, "timeout: " + what
        time.sleep(0.3)
    return time.time() - t0


nodes = [make(i) for i in range(1, 5)]
time.sleep(1)
ma = storage.publish_file(nodes[0], "data_test/a.bin")    # peer 1 shares a.bin
mb = storage.publish_file(nodes[1], "data_test/b.bin")    # peer 2 shares b.bin
t = wait_for(lambda: all(min(copies(nodes, m)) >= 2 for m in (ma, mb)), 30, "replication")
print(f"both files replicated on >= 2 peers after {t:.1f}s")
for m in (ma, mb):
    n_chunks = len(m["chunk_hashes"])
    for n in nodes:
        assert len(n.store.my_chunks(m["file_id"], n_chunks)) < n_chunks    # nobody has a complete file
print("no single peer stores a complete file")

# a new peer joins: it gets the file list and takes over its share
nodes.append(make(5))
wait_for(lambda: nodes[4].store.find_file("a.bin") is not None, 10, "file list at join")
wait_for(lambda: len(nodes[4].store.my_chunks(ma["file_id"], len(ma["chunk_hashes"]))) > 5, 20, "takeover")
print("new peer 5 got the file list and now stores", len(nodes[4].store.my_chunks(ma["file_id"], len(ma["chunk_hashes"]))), "chunks of a.bin")

# download a.bin at peer 4 while peer 3 crashes
reliable.PACKET_DELAY = 0.004
threading.Timer(1.0, lambda: (print("*** peer 3 CRASHES during the download ***"), nodes[2].stop())).start()
out = transfer.download_file(nodes[3], ma, use_local=False)
reliable.PACKET_DELAY = 0
assert out and open(out, "rb").read() == files["a.bin"]
print("download finished correctly although a source crashed")

# re-replication: chunks that lived on peer 3 get a new second copy
alive = [n for n in nodes if n.running]
t = wait_for(lambda: all(min(copies(alive, m)) >= 2 for m in (ma, mb)), 40, "re-replication")
print(f"every chunk is back on >= 2 alive peers after {t:.1f}s (re-replication)")
out = transfer.download_file(nodes[0], mb)
assert open(out, "rb").read() == files["b.bin"]
for n in nodes:
    n.stop()
shutil.rmtree("data_test", ignore_errors=True)
print("System test OK")
