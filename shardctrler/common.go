package shardctrler

import (
	"fmt"
	"strconv"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// JoinL(servers) -- add a set of groups (gid -> server-list mapping).
// LeaveL(gids) -- delete a set of groups.
// MoveL(shard, gid) -- hand off one shard from current owner to gid.
// QueryL(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK             = "OK"
	Join           = "JoinL"
	Leave          = "LeaveL"
	Move           = "MoveL"
	Query          = "QueryL"
	ErrTimeout     = "ErrTimeout"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

type CommandArgs struct {
	Op        string
	ClientId  int64
	CommandId int
	JoinArgs
	LeaveArgs
	MoveArgs
	QueryArgs
}
type CommandResp struct {
	Err
	Config
}
type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs []int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

func (cmd CommandArgs) String() string {
	args := ""
	switch cmd.Op {
	case Join:
		args += "join-servers("
		for gid, servers := range cmd.JoinArgs.Servers {
			args += strconv.Itoa(gid) + ": "
			for _, server := range servers {
				args += server + " "
			}
		}
		args += ")"
	case Leave:
		args += "leave("
		for _, gid := range cmd.LeaveArgs.GIDs {
			args += strconv.Itoa(gid) + " "
		}
		args += ")"
	case Move:
		args += "move(shard:" + strconv.Itoa(cmd.MoveArgs.Shard) + " gid:" + strconv.Itoa(cmd.MoveArgs.GID) + ")"
	case Query:
		args += "query(num:" + strconv.Itoa(cmd.QueryArgs.Num) + ")"
	}
	return fmt.Sprintf("op:%v, clientId:%v, commandId:%v, args:%v", cmd.Op, cmd.ClientId, cmd.CommandId, args)
}
