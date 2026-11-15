"""chat.py - group chat. Owner: Shahab."""
import protocol


def send_chat(channel, table, text):
    """Send text to every alive peer using channel.send_reliable."""
    pass  # TODO Day 4


def on_chat(sender_id, payload):
    print(f"[peer {sender_id}] {payload.decode()}")
