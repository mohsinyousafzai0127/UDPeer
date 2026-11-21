"""Phase 2 test.  Run: python test_storage.py"""
import os
import shutil
import tempfile

import storage

tmp = tempfile.mkdtemp()
path = os.path.join(tmp, "demo.bin")
data = os.urandom(200_000)                       # 200 KB -> 4 chunks (3 full + 1 small)
open(path, "wb").write(data)

m = storage.make_manifest(path)
assert len(m["chunk_hashes"]) == 4 and m["size"] == 200_000
assert m["sha256"] == storage.sha(data)
assert m["chunk_hashes"][0] == storage.sha(data[:65536])

# placement: same answer everywhere, 2 different owners, balanced
peers = [1, 2, 3, 4]
for c in range(4):
    a = storage.get_chunk_owners(m["file_id"], c, peers)
    assert a == storage.get_chunk_owners(m["file_id"], c, list(reversed(peers)))
    assert len(set(a)) == 2
load = {p: 0 for p in peers}
for c in range(200):
    for p in storage.get_chunk_owners(7, c, peers):
        load[p] += 1
print("chunks per peer (200 chunks, 2 copies):", load)
assert all(70 < v < 130 for v in load.values())      # roughly 100 each

# when a peer leaves, only the chunks it owned move
before = [storage.get_chunk_owners(7, c, peers) for c in range(200)]
after = [storage.get_chunk_owners(7, c, [1, 2, 3]) for c in range(200)]
moved = sum(1 for b, a in zip(before, after) if 4 not in b and b != a)
assert moved == 0

# local store
st = storage.Storage(1, base=os.path.join(tmp, "data"))
assert st.add_manifest(m) and not st.add_manifest(m)
st.sources[m["file_id"]] = path
assert st.get_chunk(m["file_id"], 3) == data[3 * 65536:]       # from the source file
assert st.load_chunk(m["file_id"], 3) is None                    # not in the store
st.save_chunk(m["file_id"], 1, data[65536:2 * 65536])
assert st.has_chunk(m["file_id"], 1) and st.my_chunks(m["file_id"], 4) == [1]
assert st.find_file("demo.bin") == m

shutil.rmtree(tmp)
print("Phase 2 OK")
