"""storage.py - files, chunks, manifests and chunk placement.  Owner: Shahab.

Ideas:
 * A file is cut into 64 KB chunks. The MANIFEST lists the SHA-256 of every chunk
   (+ of the whole file), so any peer can check what it downloads.
 * Placement = rendezvous hashing: for a chunk, every peer gets a score
   sha256(file_id-chunk_id-peer_id); the 2 peers with the highest score own the chunk.
   Every peer computes the same answer, so no coordinator is needed.
 * The publisher keeps its original file only as a SOURCE to serve from; its chunk
   store holds only the chunks it owns, so no peer's store has a complete file.
"""
import hashlib
import os
import threading

import protocol

COPIES = 2                       # every chunk lives on at least 2 peers
CHUNK_SIZE = protocol.CHUNK_SIZE


def sha(data):
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------- manifest
def make_manifest(path):
    """Read a file and build its manifest."""
    name = os.path.basename(path)
    whole = hashlib.sha256()
    hashes, size = [], 0
    with open(path, "rb") as f:
        while True:
            data = f.read(CHUNK_SIZE)
            if not data:
                break
            hashes.append(sha(data))
            whole.update(data)
            size += len(data)
    return {"file_id": int(sha(name.encode())[:4], 16),     # 16-bit id
            "name": name, "size": size,
            "sha256": whole.hexdigest(), "chunk_hashes": hashes}


# ---------------------------------------------------------------- placement
def rank_peers(file_id, chunk_id, peer_ids):
    """Peers sorted from 'best owner' to 'worst owner' for this chunk."""
    def score(pid):
        return sha(f"{file_id}-{chunk_id}-{pid}".encode())
    return sorted(peer_ids, key=score, reverse=True)


def get_chunk_owners(file_id, chunk_id, peer_ids, copies=COPIES):
    """The peers that must store this chunk."""
    return rank_peers(file_id, chunk_id, peer_ids)[:copies]


# ---------------------------------------------------------------- local store
class Storage:
    def __init__(self, my_id, base="data"):
        root = os.path.join(base, f"peer{my_id}")
        self.chunk_dir = os.path.join(root, "chunks")
        self.download_dir = os.path.join(root, "downloads")
        os.makedirs(self.chunk_dir, exist_ok=True)
        os.makedirs(self.download_dir, exist_ok=True)
        self.files = {}      # file_id -> manifest (the list of shared files)
        self.sources = {}    # file_id -> path of the original file (publisher only)
        self.lock = threading.Lock()

    # manifests
    def add_manifest(self, m):
        """True if the file was new."""
        with self.lock:
            if m["file_id"] in self.files:
                return False
            self.files[m["file_id"]] = m
            return True

    def manifests(self):
        with self.lock:
            return list(self.files.values())

    def find_file(self, text):
        """Find a manifest by name or by id."""
        for m in self.manifests():
            if m["name"] == text or str(m["file_id"]) == text:
                return m
        return None

    # chunks
    def _path(self, file_id, chunk_id):
        return os.path.join(self.chunk_dir, f"{file_id}_{chunk_id}.chunk")

    def has_chunk(self, file_id, chunk_id):
        return os.path.exists(self._path(file_id, chunk_id))

    def save_chunk(self, file_id, chunk_id, data):
        tmp = self._path(file_id, chunk_id) + ".tmp"
        with open(tmp, "wb") as f:
            f.write(data)
        os.replace(tmp, self._path(file_id, chunk_id))      # never a half-written chunk

    def load_chunk(self, file_id, chunk_id):
        try:
            with open(self._path(file_id, chunk_id), "rb") as f:
                return f.read()
        except FileNotFoundError:
            return None

    def read_source_chunk(self, file_id, chunk_id):
        """Read a chunk from the original file (only on the publisher)."""
        path = self.sources.get(file_id)
        if path is None:
            return None
        with open(path, "rb") as f:
            f.seek(chunk_id * CHUNK_SIZE)
            return f.read(CHUNK_SIZE) or None

    def get_chunk(self, file_id, chunk_id):
        """Chunk from the store, or from the original file if we published it."""
        data = self.load_chunk(file_id, chunk_id)
        return data if data is not None else self.read_source_chunk(file_id, chunk_id)

    def my_chunks(self, file_id, n_chunks):
        return [c for c in range(n_chunks) if self.has_chunk(file_id, c)]


# ---------------------------------------------------------------- publish / FILE_LIST
def publish_file(node, path):
    """Share a file with the group: build the manifest and tell every peer."""
    if not os.path.isfile(path):
        print("no such file:", path)
        return None
    m = make_manifest(path)
    payload = protocol.pack_json(m)
    if len(payload) > 60000:
        print("file too big for one manifest (max about 60 MB)")
        return None
    if not node.store.add_manifest(m):
        print("a file with this name is already shared")
        return None
    node.store.sources[m["file_id"]] = os.path.abspath(path)
    for pid, addr in node.table.get_peers().items():
        node.channel.send_async(addr, protocol.FILE_LIST, payload)
    print(f"published {m['name']}: {m['size']} bytes, {len(m['chunk_hashes'])} chunks "
          f"(sync will copy chunks to their owners)")
    return m


def handle_file_list(node, payload):
    """A peer told us about a shared file."""
    m = protocol.unpack_json(payload)
    if node.store.add_manifest(m):
        print(f"\n[new shared file: {m['name']} ({m['size']} bytes, {len(m['chunk_hashes'])} chunks)]")
