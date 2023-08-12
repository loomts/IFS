package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"IFS/labrpc"
	"IFS/shardctrler"
	"crypto/rand"
	"math/big"
	"time"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sc        *shardctrler.Clerk
	cfg       shardctrler.Config
	makeEnd   func(string) *labrpc.ClientEnd
	clientId  int64
	commandId int
	leaderId  map[int]int
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// makeEnd(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, makeEnd func(string) *labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		clientId:  nrand(),
		commandId: 0,
		leaderId:  map[int]int{},
		makeEnd:   makeEnd,
		sc:        shardctrler.MakeClerk(ctrlers),
	}
	ck.cfg = ck.sc.Query(-1)
	return ck
}

func (ck *Clerk) Command(args *CommandArgs) string {
	args.CommandId = ck.commandId
	args.ClientId = ck.clientId
	for {
		shardId := key2shard(args.Key)
		gid := ck.cfg.Shards[shardId]
		if servers, ok := ck.cfg.Groups[gid]; ok {
			oldLeader := ck.leaderId[gid]
			newLeader := oldLeader
			for {
				var reply CommandResp
				if !ck.makeEnd(servers[newLeader]).Call("ShardKV.Command", args, &reply) || reply.Err == ErrWrongLeader || reply.Err == ErrNoReady || reply.Err == ErrTimeout {
					newLeader = (newLeader + 1) % len(servers)
					if newLeader == oldLeader {
						break
					}
				} else if reply.Err == ErrWrongGroup {
					break
				} else if reply.Err == OK {
					ck.commandId++
					return reply.Value
				}
			}
		}
		time.Sleep(80 * time.Millisecond)
		ck.cfg = ck.sc.Query(-1)
	}
}

func (ck *Clerk) Get(key string) string {
	return ck.Command(&CommandArgs{Key: key, Op: GET})
}

func (ck *Clerk) Put(key string, value string) {
	ck.Command(&CommandArgs{Key: key, Value: value, Op: PUT})
}

func (ck *Clerk) Append(key string, value string) {
	ck.Command(&CommandArgs{Key: key, Value: value, Op: APPEND})
}
