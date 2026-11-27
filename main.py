"""main.py - command line of one peer.  Owner: Shahab.

Run:  python main.py <my_id> <my_port> [bootstrap_ip:port]
Ex:   python main.py 1 5000                    (first peer)
      python main.py 2 5001 127.0.0.1:5000     (joins through peer 1)
"""
import sys
import threading

import chat
import peers
import protocol
import reliable
import storage
import sync
import transfer
from node import Node

HELP = """commands:
  peers               list the peers I know
  chat <text>         send a message to the whole group
  publish <path>      share a file with the group (e.g. publish big1.bin)
  files               list shared files and how many chunks I store
  download <name>     download a shared file from several peers at once
  chunks              show which chunks I store
  loss <0..1>         simulate packet loss, e.g. loss 0.3
  slow <seconds>      slow motion: delay per packet, e.g. slow 0.004 (for the demo)
  help                this text
  leave               leave the group and quit"""


def show_files(node):
    ms = node.store.manifests()
    if not ms:
        print("no shared files yet")
    for m in ms:
        n = len(m["chunk_hashes"])
        mine = len(node.store.my_chunks(m["file_id"], n))
        print(f"  {m['name']}  {m['size']} bytes  {n} chunks  (I store {mine})")


def show_chunks(node):
    for m in node.store.manifests():
        mine = node.store.my_chunks(m["file_id"], len(m["chunk_hashes"]))
        print(f"  {m['name']}: {mine}")


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        return
    my_id, port = int(sys.argv[1]), int(sys.argv[2])
    node = Node(my_id, port)
    node.start()
    sync.start(node)
    if len(sys.argv) > 3:
        ip, p = sys.argv[3].split(":")
        peers.join(node, (ip, int(p)))
    print(f"peer {my_id} listening on UDP {port}.  Type 'help'.")

    while node.running:
        try:
            line = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            line = "leave"
        cmd, _, rest = line.partition(" ")
        try:
            if cmd == "peers":
                print(node.table.get_peers())
            elif cmd == "chat" and rest:
                if chat.send_chat(node, rest) == 0:
                    print("(no peers yet)")
            elif cmd == "publish":
                storage.publish_file(node, rest)
            elif cmd == "files":
                show_files(node)
            elif cmd == "chunks":
                show_chunks(node)
            elif cmd == "download":
                m = node.store.find_file(rest)
                if m is None:
                    print("unknown file, type 'files'")
                else:
                    threading.Thread(target=transfer.download_file, args=(node, m),
                                     daemon=True).start()
            elif cmd == "loss":
                protocol.LOSS = float(rest)
                print("packet loss =", protocol.LOSS)
            elif cmd == "slow":
                reliable.PACKET_DELAY = float(rest)
                print("delay per packet =", reliable.PACKET_DELAY)
            elif cmd == "leave":
                peers.leave(node)
            elif cmd == "help":
                print(HELP)
            elif cmd:
                print("unknown command, type 'help'")
        except Exception as e:
            print("error:", e)


if __name__ == "__main__":
    main()
