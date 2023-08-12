package shardctrler

//
// Shardctrler clerk.
//

import (
	"IFS/labrpc"
	"crypto/rand"
	"math/big"
	"time"
)

type Clerk struct {
	servers   []*labrpc.ClientEnd
	clientId  int64
	commandId int
	leaderId  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		servers:   servers,
		clientId:  nrand(),
		commandId: 0,
		leaderId:  0,
	}
	return ck
}

func (ck *Clerk) Command(args *CommandArgs) Config {
	for {
		args.CommandId = ck.commandId
		args.ClientId = ck.clientId
		var resp CommandResp
		if !ck.servers[ck.leaderId].Call("ShardCtrler.Command", args, &resp) || resp.Err == ErrWrongLeader || resp.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			time.Sleep(time.Millisecond * 60)
			continue
		}
		ck.commandId++
		return resp.Config
	}
}

func (ck *Clerk) Query(num int) Config {
	return ck.Command(&CommandArgs{Op: Query, QueryArgs: QueryArgs{Num: num}})
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.Command(&CommandArgs{Op: Join, JoinArgs: JoinArgs{Servers: servers}})
}

func (ck *Clerk) Leave(gids []int) {
	ck.Command(&CommandArgs{Op: Leave, LeaveArgs: LeaveArgs{GIDs: gids}})
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.Command(&CommandArgs{Op: Move, MoveArgs: MoveArgs{Shard: shard, GID: gid}})
}
