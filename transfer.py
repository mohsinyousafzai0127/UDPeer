"""transfer.py - chunk transfer between peers + multi-source download.  Owner: Mohsin.

Chunk transfer protocol:
  requester --CHUNK_REQUEST(file_id, chunk_id)--> owner
  owner     --CHUNK_DATA(file, chunk, index, total, 1 KB)--> requester   (x 64 packets, sliding window)
  owner without the chunk answers one CHUNK_DATA with total = 0  ("not found").
  The requester joins the packets, checks the SHA-256 from the manifest and
  only then accepts the chunk.
"""
import hashlib
import os
import queue
import threading
import time

import protocol
import storage

NO_PROGRESS = 3.0     # give up on a peer after this many seconds without a new packet
ROUNDS = 4            # how often we go through the whole peer list for one chunk


# ------------------------------------------------------------------ serving side
def handle_chunk_request(node, addr, payload):
    """Someone asked us for a chunk (runs in its own thread)."""
    file_id, chunk_id = protocol.unpack_chunk_request(payload)
    data = node.store.get_chunk(file_id, chunk_id)
    if data is None:                                    # we do not have it
        node.channel.send_reliable(addr, protocol.CHUNK_DATA,
                                   protocol.pack_chunk_data(file_id, chunk_id, 0, 0, b""))
        return
    size = protocol.PACKET_DATA_SIZE
    parts = [data[i:i + size] for i in range(0, len(data), size)]
    payloads = [protocol.pack_chunk_data(file_id, chunk_id, i, len(parts), p)
                for i, p in enumerate(parts)]
    node.channel.send_many(addr, protocol.CHUNK_DATA, payloads)


# ------------------------------------------------------------------ receiving side
def handle_chunk_data(node, sender, payload):
    """A packet of a chunk we asked for arrived (must be quick)."""
    file_id, chunk_id, index, total, data = protocol.unpack_chunk_data(payload)
    box = node.inbox.get((sender, file_id, chunk_id))
    if box is None:
        return                                          # we are not waiting for it
    if total == 0:
        box["missing"] = True
        box["event"].set()
        return
    box["packets"][index] = data
    box["total"] = total
    if len(box["packets"]) == total:
        box["event"].set()


def fetch_from_peer(node, peer_id, manifest, chunk_id):
    """Ask ONE peer for a chunk. Returns the bytes, or None (not found / died / bad hash)."""
    file_id = manifest["file_id"]
    addr = node.table.get_peers().get(peer_id)
    if addr is None:
        return None
    key = (peer_id, file_id, chunk_id)
    with node.inbox_lock:                               # two threads wanting the same chunk share one request
        box = node.inbox.get(key)
        owner = box is None
        if owner:
            box = {"packets": {}, "total": None, "missing": False, "event": threading.Event()}
            node.inbox[key] = box
    try:
        if owner:
            ok = node.channel.send_reliable(addr, protocol.CHUNK_REQUEST,
                                            protocol.pack_chunk_request(file_id, chunk_id),
                                            max_tries=12)
            if not ok:
                node.suspects[peer_id] = time.time()    # no ACK at all: probably dead
                return None
        last_count, last_change = -1, time.time()
        while not box["event"].wait(0.3):               # wait for all packets
            n = len(box["packets"])
            if n != last_count:
                last_count, last_change = n, time.time()
            elif time.time() - last_change > NO_PROGRESS:
                node.suspects[peer_id] = time.time()
                return None                             # peer stopped answering (crashed?)
            if peer_id not in node.table.get_peers():
                return None                             # peer was declared dead
        if box["missing"]:
            return None
        data = b"".join(box["packets"][i] for i in range(box["total"]))
        if hashlib.sha256(data).hexdigest() != manifest["chunk_hashes"][chunk_id]:
            return None                                 # corrupted
        return data
    finally:
        if owner:
            with node.inbox_lock:
                node.inbox.pop(key, None)


def fetch_chunk_from_group(node, manifest, chunk_id, first=0, rounds=ROUNDS):
    """Try the peers one after the other (best owner first) until one gives the chunk.
    This is the failover: a dead or empty peer simply means 'try the next replica'.
    Returns (peer_id, data) or (None, None)."""
    for attempt in range(rounds):
        order = storage.rank_peers(manifest["file_id"], chunk_id, list(node.table.get_peers()))
        if len(order) >= 2 and first % 2 == 1:          # spread the load over both replicas
            order[0], order[1] = order[1], order[0]
        recent = time.time() - 10                       # peers that just failed go to the end
        order.sort(key=lambda p: node.suspects.get(p, 0) > recent)
        for pid in order:
            data = fetch_from_peer(node, pid, manifest, chunk_id)
            if data is not None:
                return pid, data
        if attempt < rounds - 1:
            time.sleep(1)
    return None, None


# ------------------------------------------------------------------ multi-source download
def download_file(node, manifest, workers=4, use_local=True):
    """Download a whole shared file. Several worker threads fetch different chunks at the
    same time, from different peers. Returns the saved path, or None on failure."""
    t0 = time.time()
    n = len(manifest["chunk_hashes"])
    todo = queue.Queue()
    for c in range(n):
        todo.put(c)
    chunks, sources = {}, {}
    lock = threading.Lock()

    def worker():
        while True:
            try:
                c = todo.get_nowait()
            except queue.Empty:
                return
            data = node.store.get_chunk(manifest["file_id"], c) if use_local else None
            src = node.my_id
            if data is None:
                src, data = fetch_chunk_from_group(node, manifest, c, first=c)
            if data is None:
                continue                                # all replicas failed: chunk stays missing
            with lock:
                chunks[c] = data
                sources[src] = sources.get(src, 0) + 1
                if len(chunks) % 8 == 0 or len(chunks) == n:
                    print(f"  downloaded {len(chunks)}/{n} chunks")

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    missing = [c for c in range(n) if c not in chunks]
    if missing:
        print(f"download of {manifest['name']} FAILED, missing chunks: {missing}")
        return None
    data = b"".join(chunks[c] for c in range(n))
    if hashlib.sha256(data).hexdigest() != manifest["sha256"]:
        print(f"download of {manifest['name']} FAILED: file hash mismatch")
        return None
    path = os.path.join(node.store.download_dir, manifest["name"])
    with open(path, "wb") as f:
        f.write(data)
    print(f"download complete: {path} ({len(data)} bytes, {time.time() - t0:.1f}s, "
          f"integrity OK, chunks per peer: {dict(sorted(sources.items()))})")
    return path
