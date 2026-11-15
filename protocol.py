"""protocol.py - packet format used by EVERYONE. Owner: Mohsin.
After Day 1, do not change the header. Add new message types at the end only.

Header (19 bytes, network byte order / big-endian):
  version(1B) type(1B) flags(1B) sender_id(2B) seq(4B) ack(4B) length(2B) crc32(4B)
"""
import json
import random
import socket
import struct
import zlib

VERSION = 1

# ---- message types ----
JOIN = 1           # new peer -> bootstrap peer. payload: JSON {"port": 5001}
WELCOME = 2        # reply to JOIN. payload: JSON {"peers": {...}, "files": [...]}
HEARTBEAT = 3      # every 1s, unreliable, empty payload
LEAVE = 4          # peer is leaving, empty payload
CHAT = 5           # reliable. payload: UTF-8 text
ACK = 6            # header 'ack' field = seq being acknowledged, empty payload
FILE_LIST = 7      # payload: JSON manifest list (files, chunk hashes, owners)
CHUNK_REQUEST = 8  # payload: file_id(2B) chunk_id(2B)
CHUNK_DATA = 9     # payload: file_id(2B) chunk_id(2B) index(2B) total(2B) + data

# ---- flags ----
FLAG_RELIABLE = 1  # receiver must reply with ACK

# ---- settings ----
HEADER_FORMAT = "!BBBHIIHI"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)  # 19
CHUNK_SIZE = 64 * 1024        # one chunk = 64 KB
PACKET_DATA_SIZE = 1024       # one packet carries 1 KB of chunk data
LOSS = 0.0                    # set protocol.LOSS = 0.3 to simulate 30% packet loss


def pack(msg_type, sender_id, payload=b"", seq=0, ack=0, flags=0):
    """Build a packet (header + payload) with CRC32."""
    args = (VERSION, msg_type, flags, sender_id, seq, ack, len(payload))
    header = struct.pack(HEADER_FORMAT, *args, 0)
    crc = zlib.crc32(header + payload)
    return struct.pack(HEADER_FORMAT, *args, crc) + payload


def unpack(data):
    """Return a dict, or None if the packet is broken or has a bad CRC."""
    if len(data) < HEADER_SIZE:
        return None
    version, msg_type, flags, sender_id, seq, ack, length, crc = \
        struct.unpack(HEADER_FORMAT, data[:HEADER_SIZE])
    payload = data[HEADER_SIZE:]
    if version != VERSION or len(payload) != length:
        return None
    header = struct.pack(HEADER_FORMAT, version, msg_type, flags,
                         sender_id, seq, ack, length, 0)
    if zlib.crc32(header + payload) != crc:
        return None
    return {"type": msg_type, "flags": flags, "sender": sender_id,
            "seq": seq, "ack": ack, "payload": payload}


# ---- small helpers for payloads ----
def pack_json(obj):
    return json.dumps(obj).encode()


def unpack_json(payload):
    return json.loads(payload.decode())


def pack_chunk_request(file_id, chunk_id):
    return struct.pack("!HH", file_id, chunk_id)


def unpack_chunk_request(payload):
    return struct.unpack("!HH", payload)  # (file_id, chunk_id)


def pack_chunk_data(file_id, chunk_id, index, total, data):
    return struct.pack("!HHHH", file_id, chunk_id, index, total) + data


def unpack_chunk_data(payload):
    file_id, chunk_id, index, total = struct.unpack("!HHHH", payload[:8])
    return file_id, chunk_id, index, total, payload[8:]


class UdpSocket:
    """Thin UDP socket with a fake packet-loss switch."""

    def __init__(self, port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", port))

    def send(self, data, addr):
        if random.random() < LOSS:
            return  # pretend the packet got lost
        self.sock.sendto(data, addr)

    def receive(self, timeout=1.0):
        """Return (message_dict, addr), or (None, None) on timeout/bad packet."""
        self.sock.settimeout(timeout)
        try:
            data, addr = self.sock.recvfrom(65535)
        except socket.timeout:
            return None, None
        msg = unpack(data)
        if msg is None:
            return None, None
        return msg, addr

    def close(self):
        self.sock.close()
