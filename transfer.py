"""transfer.py - chunk transfer and multi-source download. Owner: Mohsin."""
import protocol


def handle_chunk_request(channel, storage, addr, payload):
    """Someone asked us for a chunk: read it from storage and send it back as
    1 KB CHUNK_DATA packets."""
    pass  # TODO Day 7


def fetch_chunk(channel, peers, owner_ids, file_id, chunk_id):
    """Ask one owner for a chunk, collect the packets, return the 64 KB bytes.
    If the owner times out, try the next owner in owner_ids."""
    pass  # TODO Day 7 / Day 9


def download_file(channel, peers, storage, file_id):
    """Download all chunks using 2+ threads, verify SHA-256 of each chunk,
    and write the final file."""
    pass  # TODO Day 8
