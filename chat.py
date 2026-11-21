"""chat.py - group chat.  Owner: Shahab.
Order and no-duplicates come from reliable.py (per-sender sequence numbers)."""
import time

import protocol

VERBOSE = True   # tests set this to False


def send_chat(node, text):
    """Send text to every alive peer. Returns how many peers it went to."""
    peers = node.table.get_peers()
    for pid, addr in peers.items():
        node.channel.send_async(addr, protocol.CHAT, text.encode())
    return len(peers)


def on_chat(node, sender, payload):
    text = payload.decode(errors="replace")
    node.chat_log.append((sender, text, time.time()))
    if VERBOSE:
        print(f"\n[peer {sender}] {text}")
