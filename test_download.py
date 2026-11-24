"""Phase 5 test: multi-source download + a peer crashes in the middle.  Run: python test_download.py
(chunks are placed by hand here; sync.py does it automatically in Phase 6)"""
import os
import shutil
import threading
import time

import chat
import peers
import protocol
import reliable
import storage
import transfer
from node import Node

chat.VERBOSE = False
shutil.rmtree("data_test", ignore_errors=True)
os.makedirs("data_test", exist_ok=True)
path = "data_test/big.bin"
data = os.urandom(2 * 1024 * 1024 + 12345)          # a bit more than 2 MB -> 33 chunks
open(path, "wb").write(data)

nodes = [Node(i, 6400 + i, "data_test") for i in range(1, 6)]
for n in nodes:
    n.start()
for n in nodes[1:]:
    peers.join(n, ("127.0.0.1", 6401))
m = storage.make_manifest(path)
for n in nodes:
    n.store.add_manifest(m)

# place every chunk on its 2 owners (what sync.py will do for real)
ids = [n.my_id for n in nodes]
for c in range(len(m["chunk_hashes"])):
    for pid in storage.get_chunk_owners(m["file_id"], c, ids):
        nodes[pid - 1].store.save_chunk(m["file_id"], c, data[c * 65536:(c + 1) * 65536])
holders = {n.my_id: len(n.store.my_chunks(m["file_id"], 33)) for n in nodes}
print("chunks stored per peer:", holders, "(nobody has all 33)")
assert max(holders.values()) < 33

# 1) plain multi-source download, peer 5 downloads without using local chunks
t0 = time.time()
out = transfer.download_file(nodes[4], m, use_local=False)
assert open(out, "rb").read() == data
print(f"download 1 OK in {time.time() - t0:.1f}s")

# 2) slow motion + crash of peer 3 in the middle of a download
reliable.PACKET_DELAY = 0.004
threading.Timer(1.0, lambda: (print("\n*** peer 3 CRASHES ***"), nodes[2].stop())).start()
t0 = time.time()
out = transfer.download_file(nodes[4], m)          # normal mode: own chunks are used
reliable.PACKET_DELAY = 0
assert out and open(out, "rb").read() == data
print(f"download 2 OK in {time.time() - t0:.1f}s even though peer 3 died")
for n in nodes:
    n.stop()
shutil.rmtree("data_test", ignore_errors=True)
print("Phase 5 OK")
