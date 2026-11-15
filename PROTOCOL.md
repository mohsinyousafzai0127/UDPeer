# Protocol (v1) - copy this into the report

Header, 19 bytes, big-endian (`!BBBHIIHI`):

| Field | Size | Meaning |
|---|---|---|
| version | 1B | always 1 |
| type | 1B | message type |
| flags | 1B | bit0 = reliable (receiver must ACK) |
| sender_id | 2B | peer id |
| seq | 4B | sequence number (per sender->receiver pair) |
| ack | 4B | in ACK packets: the seq being acknowledged |
| length | 2B | payload length |
| crc32 | 4B | CRC32 of header (crc=0) + payload |

| Type | No. | Payload | Reliable |
|---|---|---|---|
| JOIN | 1 | JSON {"port"} | yes |
| WELCOME | 2 | JSON peer list + file list | yes |
| HEARTBEAT | 3 | empty | no |
| LEAVE | 4 | empty | yes |
| CHAT | 5 | UTF-8 text | yes |
| ACK | 6 | empty | no |
| FILE_LIST | 7 | JSON manifests | yes |
| CHUNK_REQUEST | 8 | file_id(2B) chunk_id(2B) | yes |
| CHUNK_DATA | 9 | file_id(2B) chunk_id(2B) index(2B) total(2B) + up to 1024B data | yes |

Message flows:
- Join: new -> bootstrap: JOIN; bootstrap -> new: WELCOME; bootstrap tells the others.
- Chat: sender -> each peer: CHAT; peer -> sender: ACK (retransmit after 0.5s if no ACK).
- Download chunk: requester -> owner: CHUNK_REQUEST; owner -> requester: CHUNK_DATA x 64; requester checks SHA-256.
