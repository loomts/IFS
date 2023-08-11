package shardkv

import "fmt"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
	// Err
	OK             = "OK"
	ErrNoReady     = "ErrNoReady"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrOutdated    = "ErrOutdated"
	ErrTimeout     = "ErrTimeout"
)

type Err string

type CommandArgs struct {
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	ClientId  int64
	CommandId int
}

type CommandResp struct {
	Err
	Value string
}

func (cmd CommandArgs) String() string {
	return fmt.Sprintf("key:%v, value:%v, op:%v, clientId:%v, commandId:%v", cmd.Key, cmd.Value, cmd.Op, cmd.ClientId, cmd.CommandId)
}
