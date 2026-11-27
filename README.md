# UDP peer-to-peer group chat and file sharing

Serverless, UDP-only: peer discovery, group chat, reliable transfer (TCP-like, written by us),
distributed file storage (2 copies of every chunk), multi-source download, fault tolerance.
Python 3.8+, no extra libraries (only `matplotlib` for the graphs in `measure.py`).

## Files

| File | What it does |
|---|---|
| `protocol.py` | packet header, message types, pack/unpack, UDP socket with loss simulation |
| `reliable.py` | ACK, retransmission, sliding window, duplicate drop, in-order delivery |
| `peers.py` | peer table, join, heartbeat, failure detection, leave |
| `chat.py` | group chat |
| `storage.py` | chunking, manifest (SHA-256), rendezvous-hash placement, local chunk store, publish |
| `transfer.py` | chunk request/data, multi-source download, failover to another replica |
| `sync.py` | gives joining peers their chunks, re-replicates chunks of dead peers |
| `node.py` | glue: one peer = channel + table + storage + message dispatcher |
| `main.py` | command line |
| `measure.py` | experiments + graphs for the report |
| `test_*.py` | one test per phase |

## Run (demo with 4 peers)

```
python make_testfiles.py                 # creates big1.bin and big2.bin (3 MB each)
```
Open 4 terminals in this folder:
```
python main.py 1 5000
python main.py 2 5001 127.0.0.1:5000
python main.py 3 5002 127.0.0.1:5000
python main.py 4 5003 127.0.0.1:5000
```
On different laptops use the real IP of peer 1: `python main.py 2 5001 192.168.1.10:5000`
(allow Python through the firewall; campus Wi-Fi sometimes blocks peer-to-peer traffic: use a phone hotspot).

Commands: `peers`, `chat <text>`, `publish <file>`, `files`, `chunks`, `download <name>`, `loss <0..1>`, `slow <sec>`, `leave`.

Demo script (the kill-during-download scenario):
1. peer 1: `publish big1.bin`; peer 2: `publish big2.bin`
2. any peer: `files`, `chunks` -> every chunk is on 2 peers, nobody has a complete file
3. all peers: `slow 0.004` (so a 3 MB download takes several seconds), peer 4: `download big1.bin`
4. while it runs, close the terminal of peer 3 (or Ctrl+C)
5. peer 4 prints `download complete ... integrity OK` (it switched to the other replica)
6. the other peers print `[peer 3 timed out - removed]` and `[sync] now storing ...` (re-replication)
Also try `loss 0.3` and `chat hello` to show reliable chat under packet loss.

Delete the `data/` folder to start from scratch.

## Tests and measurements

```
python test_protocol.py && python test_reliable.py && python test_storage.py
python test_membership.py && python test_transfer.py && python test_download.py && python test_system.py
python measure.py          # results/*.csv and results/*.png (about 5 minutes)
```
