"""Phase 4 test: fetch chunks from another peer under packet loss.  Run: python test_transfer.py"""
import hashlib
import os
import shutil
import time

import chat
import peers
import protocol
import storage
import transfer
from node import Node

chat.VERBOSE = False
shutil.rmtree("data_test", ignore_errors=True)
os.makedirs("data_test", exist_ok=True)
path = "data_test/sample.bin"
data = os.urandom(300_000)                       # 5 chunks
open(path, "wb").write(data)

a, b = Node(1, 6301, "data_test"), Node(2, 6302, "data_test")
a.start(); b.start()
peers.join(b, ("127.0.0.1", 6301))
m = storage.publish_file(a, path)                # a is the publisher (serves from the original file)
time.sleep(1)
assert b.store.find_file("sample.bin") == m      # b got the file list

protocol.LOSS = 0.2
t0 = time.time()
for c in range(5):
    src, chunk = transfer.fetch_chunk_from_group(b, m, c)
    assert src == 1 and chunk == data[c * 65536:(c + 1) * 65536], c
protocol.LOSS = 0.0
print(f"5 chunks fetched and hash-checked at 20% packet loss in {time.time() - t0:.1f}s")

# a chunk nobody has -> 'not found' -> None
fake = dict(m, file_id=m["file_id"] + 1)
fake["chunk_hashes"] = m["chunk_hashes"]
assert transfer.fetch_chunk_from_group(b, fake, 0, rounds=1) == (None, None)
print("missing chunk correctly reported as not found")
a.stop(); b.stop()
shutil.rmtree("data_test", ignore_errors=True)
print("Phase 4 OK")
