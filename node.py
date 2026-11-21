"""node.py - one peer = channel + peer table + storage + message dispatcher.
Shared by both of us (this is only glue code)."""
import threading

import chat
import peers
import protocol
import storage
import transfer
from reliable import ReliableChannel


class Node:
    def __init__(self, my_id, port, base="data"):
        self.my_id = my_id
        self.port = port
        self.running = True
        self.table = peers.PeerTable()
        self.store = storage.Storage(my_id, base)
        self.channel = ReliableChannel(my_id, port, self.on_message)
        self.welcomed = threading.Event()
        self.contacts = {}                   # peers learned from WELCOME (to send JOIN to)
        self.chat_log = []                   # (sender, text, time)
        self.inbox = {}                      # chunk downloads in progress
        self.inbox_lock = threading.Lock()
        self.suspects = {}                   # peer_id -> time when a chunk request to it failed

    def all_ids(self):
        """Ids of all alive peers including me."""
        return sorted(list(self.table.get_peers()) + [self.my_id])

    def start(self):
        self.channel.start()
        threading.Thread(target=peers.heartbeat_loop, args=(self,), daemon=True).start()

    def stop(self):
        self.running = False
        self.channel.stop()

    def on_message(self, msg_type, sender, payload, addr):
        """Called by the channel for every delivered message."""
        if msg_type == protocol.JOIN:
            peers.handle_join(self, sender, addr)
        elif msg_type == protocol.WELCOME:
            peers.handle_welcome(self, sender, addr, payload)
        elif msg_type == protocol.HEARTBEAT:
            peers.handle_heartbeat(self, sender, addr)
        elif msg_type == protocol.LEAVE:
            peers.handle_leave(self, sender)
        elif msg_type == protocol.CHAT:
            chat.on_chat(self, sender, payload)
        elif msg_type == protocol.FILE_LIST:
            storage.handle_file_list(self, payload)
        elif msg_type == protocol.CHUNK_REQUEST:      # serving a chunk can take a while -> own thread
            threading.Thread(target=transfer.handle_chunk_request,
                             args=(self, addr, payload), daemon=True).start()
        elif msg_type == protocol.CHUNK_DATA:
            transfer.handle_chunk_data(self, sender, payload)
