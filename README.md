## IFS
### TODO 
- 6.824 Raft refactor
- [ ] gRPC interface design
- [ ] rpc util
- [ ] config
- [ ] raft refactor
- [ ] client
- [ ] ctrler server
- [ ] kv server
- [ ] persistence(RocksDB)
- IFS interface design
- [ ] gRPC interface design
- Caddy file manager

### Image File System interfaces

- Get image by name or path
- Get images by topic or path
- Post image by name or path [with topic]
- Get topics
- Get paths
- Delete a image, rename a image

### feature

1. Use Raft to ensure data consistency, Load balance and Fault tolerance.
2. Horizontal expansion
3. Web & API
4. WAL & Snapshot & KV

### Architectural

client, config server, chunk server

### Reference
https://github.com/tikv/tikv

https://github.com/seaweedfs/seaweedfs

https://github.com/qinguoyi/osproxy/tree/main

https://github.com/Kirov7/FayDFS

https://github.com/colinmarc/hdfs

https://github.com/etcd-io/etcd