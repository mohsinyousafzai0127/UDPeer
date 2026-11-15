"""main.py - command line. Owner: Shahab.

Run:  python main.py <my_id> <my_port> [bootstrap_ip:port]
Ex:   python main.py 1 5000
      python main.py 2 5001 127.0.0.1:5000
"""
import sys

import chat
import peers
import protocol
from reliable import ReliableChannel


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        return
    my_id, port = int(sys.argv[1]), int(sys.argv[2])
    bootstrap = None
    if len(sys.argv) > 3:
        ip, p = sys.argv[3].split(":")
        bootstrap = (ip, int(p))

    table = peers.PeerTable()

    def on_message(msg_type, sender_id, payload, addr):
        if msg_type == protocol.CHAT:
            chat.on_chat(sender_id, payload)
        # TODO: JOIN, WELCOME, HEARTBEAT, LEAVE, CHUNK_REQUEST ...

    channel = ReliableChannel(my_id, port, on_message)
    channel.start()
    if bootstrap:
        peers.join(channel, table, bootstrap)

    print("Commands: peers | chat <text> | files | download <file> | leave")
    while True:
        line = input("> ").strip()
        if line == "peers":
            print(table.get_peers())
        elif line.startswith("chat "):
            chat.send_chat(channel, table, line[5:])
        elif line == "leave":
            break  # TODO send LEAVE to everyone
    channel.stop()


if __name__ == "__main__":
    main()
