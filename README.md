---
title: "IDFS"
date: 2023-08-05T14:51:11+08:00
draft: true
---

## 文件系统
GFS、HDFS、NAS、TFS、FastgDFS、Facebook Haystack、seaweedfs
单机文件系统存在存储容量和读写性能的上限，并且单机文件系统如果遇到磁盘损坏等问题将会造成非常大的损失。

## GFS

huge
parallel -> sharding
faults -> tolerance
consistencies -> replication -> low performance
recovery

## IFS

Image File System interfaces

- Open a file, check status of a file, close a file
- Read data from a file
- Write data to a file
- List files in a directory, create/delete a directory
- Delete a file, rename a file, add a symlink to a file

### feature

哈希、负载均衡、动态扩缩容、容错

### 架构

client, config server, chunk server

单个 master，维护所有元数据

### 高可用设计

校验和校验
副本复制（chunkserver 宕机）
负载均衡（master 定期对 chunkserver 监测，负载过高则搬迁）

### 流水线

### 数据流和控制流相分离

### 文件的元数据

1. 文件和 chunk 的 namespace[持久化]
2. 文件到 chunk 的映射[持久化]
3. 每个 chunk 的位置[不持久化]

# 需要支持的特性
- 分布式架构
- 重点对图片进行优化
- 并发处理能力
- 随机访问延迟较低
- 备份
- 提供 RESTFul、POSIX、FUSE、S3，HDFS（能够作为 hadoop 和 spark 的后端存储）接口
- 文件操作原子性
- 删纠码
- 分层存储
- 负载均衡
- 快照
- Volume，Object
