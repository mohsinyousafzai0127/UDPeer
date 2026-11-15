"""storage.py - chunking, manifest, placement. Owner: Shahab."""
import hashlib
import protocol


def make_manifest(path, file_id):
    """Split a file into 64 KB chunks. Return
    {"file_id", "name", "size", "chunk_hashes": [sha256 hex, ...]}."""
    pass  # TODO Day 6


def get_chunk_owners(file_id, chunk_id, peer_ids, copies=2):
    """Rendezvous hashing: rank peers by sha256(f'{file_id}-{chunk_id}-{peer}')
    and return the top `copies` peer ids. Every peer gets the same answer."""
    pass  # TODO Day 7


def save_chunk(peer_id, file_id, chunk_id, data):
    pass  # TODO Day 6


def load_chunk(peer_id, file_id, chunk_id):
    pass  # TODO Day 6
