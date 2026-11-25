"""sync.py - keeps every chunk on its 2 owners.  Owner: Shahab.

Every 1.5 s each peer checks: "which chunks should I own (rendezvous hashing over the
CURRENT peers) that I do not have yet?" and fetches them from whoever has them.
This one rule gives us three features:
  * initial distribution: after a file is published, owners pull their chunks from the publisher
  * joining peer takes over its share of chunk storage
  * re-replication: a peer died -> its chunks get new owners -> they copy from the survivors
"""
import threading
import time

import storage
import transfer

CHECK_EVERY = 1.5


def rebalance(node):
    """One pass. Returns the number of chunks fetched."""
    ids = node.all_ids()
    total = 0
    for m in node.store.manifests():
        fid, got = m["file_id"], 0
        for c in range(len(m["chunk_hashes"])):
            if not node.running:
                return total
            if node.my_id not in storage.get_chunk_owners(fid, c, ids):
                continue
            if node.store.has_chunk(fid, c):
                continue
            data = node.store.read_source_chunk(fid, c)           # we published it
            if data is None:
                _, data = transfer.fetch_chunk_from_group(node, m, c, rounds=1)
            if data is not None:
                node.store.save_chunk(fid, c, data)
                got += 1
        if got:
            print(f"\n[sync] now storing {got} more chunks of {m['name']}")
        total += got
    return total


def sync_loop(node):
    while node.running:
        try:
            rebalance(node)
        except Exception as e:
            print("[sync error]", e)
        time.sleep(CHECK_EVERY)


def start(node):
    threading.Thread(target=sync_loop, args=(node,), daemon=True).start()
