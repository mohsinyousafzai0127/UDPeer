"""main.py - command line of one peer.  Owner: Shahab.

Run:  python main.py <my_id> <my_port> [bootstrap_ip:port]
Ex:   python main.py 1 5000                    (first peer)
      python main.py 2 5001 127.0.0.1:5000     (joins through peer 1)
"""
import sys

import chat
import peers
import protocol
import reliable
from node import Node

HELP = """commands:
  peers             list the peers I know
  chat <text>       send a message to the whole group
  loss <0..1>       simulate packet loss, e.g. loss 0.3
  help              this text
  leave             leave the group and quit"""


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        return
    my_id, port = int(sys.argv[1]), int(sys.argv[2])
    node = Node(my_id, port)
    node.start()
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
            elif cmd == "loss":
                protocol.LOSS = float(rest)
                print("packet loss =", protocol.LOSS)
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
