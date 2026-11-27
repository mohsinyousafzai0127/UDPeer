# Application-layer protocol, version 1

All numbers are big-endian (network byte order). One UDP datagram = one packet.

## Header (19 bytes, struct format `!BBBHIIHI`)

| Field | Size | Meaning |
|---|---|---|
| version | 1 B | always 1 |
| type | 1 B | message type (table below) |
| flags | 1 B | bit 0 = RELIABLE (receiver must send an ACK) |
| sender_id | 2 B | id of the sending peer |
| seq | 4 B | sequence number, per (sender -> receiver) pair, reliable packets only |
| ack | 4 B | in an ACK packet: the seq that is acknowledged |
| length | 2 B | payload length in bytes |
| crc32 | 4 B | CRC-32 of header (with crc field = 0) + payload; bad packets are dropped |

## Message types

| Type | No. | Reliable | Payload |
|---|---|---|---|
| JOIN | 1 | yes | JSON `{}`; starts a session |
| WELCOME | 2 | yes | JSON `{"peers": {"<id>": [ip, port], ...}}`; starts a session |
| HEARTBEAT | 3 | no | empty |
| LEAVE | 4 | yes | empty |
| CHAT | 5 | yes | UTF-8 text |
| ACK | 6 | no | empty (`ack` field = acknowledged seq) |
| FILE_LIST | 7 | yes | JSON manifest `{"file_id","name","size","sha256","chunk_hashes":[...]}` (one file per message) |
| CHUNK_REQUEST | 8 | yes | `file_id`(2 B) `chunk_id`(2 B) |
| CHUNK_DATA | 9 | yes | `file_id`(2 B) `chunk_id`(2 B) `index`(2 B) `total`(2 B) + up to 1024 B data. `total = 0` means "I do not have this chunk" |

## Reliability rules (TCP-like, no congestion control)

* Every reliable packet is ACKed individually. The sender keeps up to 32 un-ACKed packets in flight (sliding window) and resends a packet after RTO = 0.2 s (max 30 tries).
* The receiver ACKs every reliable packet (also duplicates), drops duplicates, buffers early packets and delivers **in order per sender**.
* The first reliable packet between two peers is JOIN (from the joiner) or WELCOME (the answer). Such a packet *starts a new session* at the receiver: the expected seq is set to the packet's seq. Initial seq numbers are random, so a peer that restarts is accepted again.
* Reliable packets from an unknown sender (no session yet) are not ACKed; the sender retries.

## Message sequences

**Join**
```
N -> B : JOIN                      B = bootstrap peer (any peer already in the group)
B -> N : WELCOME(peer list)        B adds N to its table
B -> N : FILE_LIST x (one per shared file)
N -> P : JOIN        for every other peer P in the list
P -> N : WELCOME, FILE_LIST...     everybody now knows everybody; B is not needed any more
```
**Membership**: every peer sends HEARTBEAT to all peers every 1 s. A peer silent for 6 s is removed. LEAVE removes a peer immediately.

**Chat**: `CHAT` to every peer, each answered by `ACK`; retransmission until ACKed.

**Publish**: publisher builds the manifest, sends `FILE_LIST` to all. Every peer computes the 2 owners of each chunk (rendezvous hashing over the alive peers) and the owners fetch the chunks they are missing (`CHUNK_REQUEST` / `CHUNK_DATA`).

**Download chunk**
```
R -> O : CHUNK_REQUEST(file, chunk)
O -> R : CHUNK_DATA(index 0..63, total 64)   sliding window, each packet ACKed
R      : joins the 64 packets, checks SHA-256 against the manifest
```
If O does not answer for 3 s or is declared dead, R asks the next peer in rendezvous order.
