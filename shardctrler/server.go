package shardctrler

import (
	"IFS/raft"
	"IFS/utils"
	"encoding/gob"
	"sort"
	"sync/atomic"
	"time"
)
import "IFS/labrpc"
import "sync"

type ShardCtrler struct {
	mu          sync.Mutex
	me          int
	rf          *raft.Raft
	applyCh     chan raft.ApplyMsg
	dead        int32
	configs     []Config // indexed by config num
	log         map[string]string
	lastApplied int
	lastLog     map[int64]int
	cmdChan     map[int]chan CommandResp
}

const applyTimeout = time.Millisecond * 100

func (sc *ShardCtrler) getChL(logIndex int) chan CommandResp {
	if sc.cmdChan[logIndex] == nil {
		sc.cmdChan[logIndex] = make(chan CommandResp, 1)
	}
	return sc.cmdChan[logIndex]
}

func (sc *ShardCtrler) isDuplicatedL(clientId int64, commandId int) bool {
	if lastIndex, ok := sc.lastLog[clientId]; ok {
		if lastIndex >= commandId {
			return true
		}
	}
	return false
}

func (sc *ShardCtrler) Command(args *CommandArgs, resp *CommandResp) {
	utils.DPrintf(utils.DServer, "S%v <- C%v [%v]", sc.me, args.ClientId, *args)
	sc.mu.Lock()
	if args.Op != Query && sc.isDuplicatedL(args.ClientId, args.CommandId) {
		sc.mu.Unlock()
		utils.DPrintf(utils.DWarn, "S%v <- C%v cmd duplicated", sc.me, args.ClientId)
		resp.Err = OK
		return
	}
	sc.mu.Unlock()
	index, _, successful := sc.rf.Start(*args)
	if !successful {
		resp.Err = ErrWrongLeader
		utils.DPrintf(utils.DWarn, "S%v <- C%v cmd wrongLeader", sc.me, args.ClientId)
		return
	}
	sc.mu.Lock()
	ch := sc.getChL(index)
	sc.mu.Unlock()
	select {
	case <-time.After(applyTimeout):
		utils.DPrintf(utils.DServer, "S%v <- C%v cmd timeout", sc.me, args.ClientId)
		resp.Err = ErrTimeout
	case reply := <-ch:
		resp.Err = OK
		utils.DPrintf(utils.DServer, "S%v -> C%v-%v %v(%v):%v", sc.me, args.ClientId, args.CommandId, args.Op, args.Num, reply)
		if args.Op == Query {
			resp.Config = reply.Config
		}
	}
}
func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		select {
		case msg := <-sc.applyCh:
			if msg.CommandValid {
				cmd := msg.Command.(CommandArgs)
				utils.DPrintf(utils.DServer, "S%v apply cmd[%v]", sc.me, cmd)
				sc.mu.Lock()
				if msg.CommandIndex <= sc.lastApplied {
					sc.mu.Unlock()
					utils.DPrintf(utils.DServer, "S%v applied commandIndex%v", sc.me, msg.CommandIndex)
					continue
				}
				if cmd.Op == Query || !sc.isDuplicatedL(cmd.ClientId, cmd.CommandId) {
					sc.lastApplied = msg.CommandIndex
					resp := CommandResp{}
					switch cmd.Op {
					case Join:
						resp = sc.JoinL(cmd.JoinArgs.Servers)
					case Leave:
						resp = sc.LeaveL(cmd.LeaveArgs.GIDs)
					case Move:
						resp = sc.MoveL(cmd.MoveArgs.Shard, cmd.MoveArgs.GID)
					case Query:
						resp = sc.QueryL(cmd.QueryArgs.Num)
					}
					if term, isLeader := sc.rf.GetState(); isLeader && term == msg.CommandTerm {
						ch := sc.getChL(msg.CommandIndex)
						ch <- resp
						close(ch)
						delete(sc.cmdChan, msg.CommandIndex)
					}
				}
				sc.mu.Unlock()
			}
		}
	}
}

func (sc *ShardCtrler) ShardsFiter(Shards *[NShards]int, Newgids *[]int) map[int][]int {
	newgids := *Newgids
	shards := *Shards
	g2s := make(map[int][]int)
	if len(newgids) > 0 {
		for _, newgid := range newgids {
			g2s[newgid] = []int{}
		}
	}
	for i := 0; i < NShards; i++ {
		g2s[shards[i]] = append(g2s[shards[i]], i)
	}
	//fmt.Printf("S%v ShardsFiter prev g2s:%v\n", sc.me, g2s)
	if shards, ok := g2s[0]; ok {
		ma := 0
		for i, _ := range g2s {
			if i > ma {
				ma = i
			}
		}
		if ma != 0 {
			g2s[ma] = append(g2s[ma], shards...)
		}
		delete(g2s, 0)
	}
	for {
		maid, ma, miid, mi := -1, -1, -1, 1000000009
		for gid, shards := range g2s {
			if len(shards) > ma || (len(shards) == ma && gid > maid) {
				ma = len(shards)
				maid = gid
			}
			if len(shards) < mi || (len(shards) == mi && gid < miid) {
				mi = len(shards)
				miid = gid
			}
		}
		if ma-mi <= 1 {
			break
		}
		sort.Ints(g2s[maid])
		g2s[miid] = append(g2s[miid], g2s[maid][0])
		g2s[maid] = g2s[maid][1:]
	}
	//fmt.Printf("S%v ShardsFiter after g2s:%v\n", sc.me, g2s)
	return g2s
}
func (sc *ShardCtrler) JoinL(groups map[int][]string) CommandResp {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{len(sc.configs), lastConfig.Shards, utils.DeepCopy(lastConfig.Groups).(map[int][]string)}
	var newgids []int
	for gid, servers := range groups {
		if _, ok := newConfig.Groups[gid]; !ok {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newConfig.Groups[gid] = newServers
		}
		newgids = append(newgids, gid)
	}
	g2s := sc.ShardsFiter(&newConfig.Shards, &newgids)
	var newShards [NShards]int
	for gid, shards := range g2s {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	newConfig.Shards = newShards
	sc.configs = append(sc.configs, newConfig)
	return CommandResp{OK, Config{}}
}

func (sc *ShardCtrler) LeaveL(gids []int) CommandResp {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{len(sc.configs), lastConfig.Shards, utils.DeepCopy(lastConfig.Groups).(map[int][]string)}

	g2s := sc.ShardsFiter(&newConfig.Shards, &[]int{})
	//fmt.Printf("LeaveL g2s:%v\n", g2s)

	orphanShards := make([]int, 0)
	for _, gid := range gids {
		if _, ok := newConfig.Groups[gid]; ok {
			delete(newConfig.Groups, gid)
		}
		if shards, ok := g2s[gid]; ok {
			orphanShards = append(orphanShards, shards...)
			delete(g2s, gid)
		}
	}
	var newShards [NShards]int
	shardingGids := make(map[int]bool)
	for gid, shards := range g2s {
		shardingGids[gid] = true
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	//fmt.Printf("S%v LeaveL prev shards:%v\n", sc.me, newShards)
	orphanGids := make([]int, 0)
	for gid, _ := range newConfig.Groups {
		if shardingGids[gid] == false {
			orphanGids = append(orphanGids, gid)
		}
	}
	g2s = sc.ShardsFiter(&newShards, &orphanGids)

	if len(newConfig.Groups) != 0 {
		for gid, shards := range g2s {
			for _, shard := range shards {
				newShards[shard] = gid
			}
		}
	}
	//fmt.Printf("S%v LeaveL after shards:%v\n", sc.me, newShards)

	newConfig.Shards = newShards
	sc.configs = append(sc.configs, newConfig)
	return CommandResp{OK, Config{}}
}
func (sc *ShardCtrler) MoveL(shardid int, gid int) CommandResp {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{len(sc.configs), lastConfig.Shards, utils.DeepCopy(lastConfig.Groups).(map[int][]string)}
	var newShards [NShards]int
	for i := 0; i < NShards; i++ {
		newShards[i] = newConfig.Shards[i]
	}
	newShards[shardid] = gid
	g2s := sc.ShardsFiter(&newShards, &[]int{})
	if len(newConfig.Groups) != 0 {
		for gid, shards := range g2s {
			for _, shard := range shards {
				newShards[shard] = gid
			}
		}
	}
	newConfig.Shards = newShards
	sc.configs = append(sc.configs, newConfig)
	return CommandResp{OK, Config{}}
}
func (sc *ShardCtrler) QueryL(num int) CommandResp {
	id := num
	if len(sc.configs) <= num || num == -1 {
		id = len(sc.configs) - 1
	}
	return CommandResp{OK, sc.configs[id]}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	utils.DPrintf(utils.DDrop, "S%v dead", sc.me)
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardsc tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	gob.Register(CommandArgs{})
	gob.Register(CommandResp{})
	sc := &ShardCtrler{
		mu:          sync.Mutex{},
		me:          me,
		rf:          nil,
		dead:        0,
		applyCh:     make(chan raft.ApplyMsg),
		configs:     make([]Config, 1),
		log:         make(map[string]string),
		lastApplied: 0,
		lastLog:     make(map[int64]int),
		cmdChan:     make(map[int]chan CommandResp),
	}
	sc.configs[0].Groups = make(map[int][]string)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	go sc.applier()
	return sc
}
