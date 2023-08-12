package shardkv

import (
	"IFS/labrpc"
	"IFS/raft"
	"IFS/shardctrler"
	"bytes"
	"encoding/gob"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	applyTimeout = time.Second
	// Command args
	Operation     = "Operation"
	Configuration = "Configuration"
	AppendShards  = "AppendShards"
	DeleteShards  = "DeleteShards"
	EmptyEntry    = "EmptyEntry"
)

const (
	Serving = iota
	Pushing
	Pulling
)

const (
	updateCfgTimeout = time.Millisecond * 80
	sendEmptyTimeout = time.Millisecond * 100
)

type ShardKV struct {
	mu               sync.Mutex
	me               int
	rf               *raft.Raft
	applyCh          chan raft.ApplyMsg
	makeEnd          func(string) *labrpc.ClientEnd
	gid              int
	ctrlers          []*labrpc.ClientEnd
	maxraftstate     int // snapshot if stateMachine grows this big
	dead             int32
	lastcfg          shardctrler.Config
	cfg              shardctrler.Config
	sc               *shardctrler.Clerk
	lastApplied      int
	lastIncludeIndex int
	stateMachine     map[int]*Shard
	commandReplyCh   map[int]chan *CommandResp
}

type Command struct {
	Op   string
	Data interface{}
}

type Shard struct {
	Log         map[string]string
	LastCommand map[int64]int
	Status      int
}

type ShardArgs struct {
	CfgNum   int
	ShardIds []int
	Shards   map[int]*Shard
}

type ShardResp struct {
	Err
}

func (args ShardArgs) String() string {
	return fmt.Sprintf("CfgNum:%v, shards:%v", args.CfgNum, args.Shards)
}

func (resp ShardResp) String() string {
	return fmt.Sprintf("err:%v", resp.Err)
}

func (shard Shard) String() string {
	status := "ERR STATUS"
	if shard.Status == Serving {
		status = "Serving"
	} else if shard.Status == Pushing {
		status = "Pushing"
	} else if shard.Status == Pulling {
		status = "Pulling"
	}
	log := "("
	for k, v := range shard.Log {
		log += k + ":" + v
	}
	log += ")"
	lastcommit := "("
	for k, v := range shard.LastCommand {
		lastcommit += strconv.Itoa(int(k)) + ":" + strconv.Itoa(v)
	}
	return fmt.Sprintf("status:%v, log:%v, lastcommit:%v", status, log, lastcommit)
}

func (kv *ShardKV) getChL(commandId int) chan *CommandResp {
	if kv.commandReplyCh[commandId] == nil {
		kv.commandReplyCh[commandId] = make(chan *CommandResp, 1)
	}
	return kv.commandReplyCh[commandId]
}

func (kv *ShardKV) isDuplicatedL(shardId int, clientId int64, commandId int) bool {
	if lastIndex, ok := kv.stateMachine[shardId].LastCommand[clientId]; ok {
		if lastIndex >= commandId {
			return true
		}
	}
	return false
}

func (kv *ShardKV) Command(args *CommandArgs, reply *CommandResp) {
	DPrintf(DTemp, "S%v gid:%v <- C%v-%v shard:%v [%v]", kv.me, kv.gid, args.ClientId, args.CommandId, key2shard(args.Key), *args)
	kv.mu.Lock()
	if !kv.serverable(key2shard(args.Key)) {
		DPrintf(DTrace, "S%v gid:%v k2shard:%v -> C%v-%v err wrong group cfg:%v", kv.me, kv.gid, key2shard(args.Key), args.ClientId, args.CommandId, kv.cfg)
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	if args.Op != GET && kv.isDuplicatedL(key2shard(args.Key), args.ClientId, args.CommandId) {
		kv.mu.Unlock()
		DPrintf(DTrace, "S%v gid:%v -> C%v cmd duplicated", kv.me, kv.gid, args.ClientId)
		reply.Err = OK
		return
	}
	kv.mu.Unlock()

	index, _, successful := kv.rf.Start(Command{Operation, *args})
	if !successful {
		reply.Err = ErrWrongLeader
		DPrintf(DTrace, "S%v gid:%v -> C%v cmd wrongLeader", kv.me, kv.gid, args.ClientId)
		return
	}
	kv.mu.Lock()
	ch := kv.getChL(index)
	kv.mu.Unlock()
	select {
	case <-time.After(applyTimeout):
		kv.mu.Lock()
		DPrintf(DTrace, "S%v gid:%v -> C%v cmd timeout, %v", kv.me, kv.gid, args.ClientId, kv.stateMachine[key2shard(args.Key)])
		reply.Err = ErrTimeout
		kv.mu.Unlock()
	case resp := <-ch:
		kv.mu.Lock()
		reply.Err = resp.Err
		DPrintf(DTrace, "S%v gid:%v -> C%v-%v %v(%v)=%v", kv.me, kv.gid, args.ClientId, args.CommandId, args.Op, args.Key, resp.Value)
		if args.Op == GET {
			reply.Value = resp.Value
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) applier() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid && !kv.killed() {
				kv.mu.Lock()
				if msg.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = msg.CommandIndex
				cmd := msg.Command.(Command)
				reply := CommandResp{}
				switch cmd.Op {
				case Operation:
					operation := cmd.Data.(CommandArgs)
					reply = kv.applyOperationL(&operation)
				case Configuration:
					nextConfig := cmd.Data.(shardctrler.Config)
					kv.applyConfigurationL(&nextConfig)
				case AppendShards:
					shardsInfo := cmd.Data.(ShardArgs)
					reply = kv.applyAppendShardsL(&shardsInfo)
				case DeleteShards:
					shardsInfo := cmd.Data.(ShardArgs)
					kv.applyDeleteShardsL(&shardsInfo)
				case EmptyEntry:
					kv.applyEmptyEntryL()
				}
				DPrintf(DInfo, "S%v gid:%v apply index:%v", kv.me, kv.gid, msg.CommandIndex)
				if term, isLeader := kv.rf.GetState(); isLeader && term == msg.CommandTerm {
					ch := kv.getChL(msg.CommandIndex)
					ch <- &reply
					close(ch)
					delete(kv.commandReplyCh, msg.CommandIndex)
				}
				if kv.maxraftstate != -1 && kv.rf.Size() > kv.maxraftstate {
					kv.snapshotL(kv.lastApplied)
					DPrintf(DSnap, "S%v gid:%v snapshoted", kv.me, kv.gid)
				}
				kv.mu.Unlock()
			} else if msg.SnapshotValid && !kv.killed() {
				kv.mu.Lock()
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					kv.readPersistL(msg.Snapshot)
					DPrintf(DSnap, "S%v gid:%v installed snapshot", kv.me, kv.gid)
				}
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *ShardKV) applyOperationL(cmd *CommandArgs) CommandResp {
	shardId := key2shard(cmd.Key)
	if kv.serverable(shardId) {
		if cmd.Op != GET && !kv.isDuplicatedL(shardId, cmd.ClientId, cmd.CommandId) {
			kv.stateMachine[shardId].LastCommand[cmd.ClientId] = cmd.CommandId
			if cmd.Op == PUT {
				kv.stateMachine[shardId].Log[cmd.Key] = cmd.Value
			} else if cmd.Op == APPEND {
				kv.stateMachine[shardId].Log[cmd.Key] += cmd.Value
			}
		}
		return CommandResp{OK, kv.stateMachine[shardId].Log[cmd.Key]}
	}
	return CommandResp{ErrWrongGroup, ""}
}

func (kv *ShardKV) updateCfg() {
	kv.mu.Lock()
	currCfgNum := kv.cfg.Num
	if !kv.allSentL() {
		kv.pushShardsL()
	}
	DPrintf(DCommit, "S%v gid:%v allSent", kv.me, kv.gid)
	if !kv.allReceivedL() {
		kv.mu.Unlock()
		return
	}
	DPrintf(DCommit, "S%v gid:%v allReceived", kv.me, kv.gid)
	kv.mu.Unlock()
	newCfg := kv.sc.Query(currCfgNum + 1)
	if newCfg.Num == currCfgNum+1 {
		kv.rf.Start(Command{Configuration, newCfg})
	}
}

func (kv *ShardKV) allSentL() bool {
	for shard, gid := range kv.lastcfg.Shards {
		if gid == kv.gid && kv.cfg.Shards[shard] != kv.gid && kv.stateMachine[shard].Status != Serving {
			return false
		}
	}
	return true
}

func (kv *ShardKV) allReceivedL() bool {
	for shard, gid := range kv.lastcfg.Shards {
		if gid != kv.gid && kv.cfg.Shards[shard] == kv.gid && kv.stateMachine[shard].Status != Serving {
			return false
		}
	}
	return true
}

func (kv *ShardKV) applyConfigurationL(cfg *shardctrler.Config) CommandResp {
	DPrintf(DError, "S%v gid:% cfgNum:%v myCfgNum:%v", kv.me, kv.gid, cfg.Num, kv.cfg.Num)
	if cfg.Num == kv.cfg.Num+1 {
		kv.lastcfg = kv.cfg
		kv.cfg = *cfg
		// pushing to newer gid
		for i := 0; i < shardctrler.NShards; i++ {
			if kv.lastcfg.Shards[i] != kv.cfg.Shards[i] {
				if kv.lastcfg.Shards[i] == kv.gid {
					kv.stateMachine[i].Status = Pushing
				}
				if kv.cfg.Shards[i] == kv.gid {
					if kv.lastcfg.Shards[i] == 0 {
						// if kv is the first one to handle
						kv.stateMachine[i] = &Shard{Log: make(map[string]string), LastCommand: make(map[int64]int), Status: Serving}
						DPrintf(DError, "S%v gid:%v shard[%v] Serving", kv.me, kv.gid, i)
					} else {
						kv.stateMachine[i] = &Shard{Status: Pulling}
						DPrintf(DError, "S%v gid:%v shard[%v] Pulling", kv.me, kv.gid, i)
					}
				}
			}
		}
		DPrintf(DApply, "S%v gid:%v applying CfgNum:%v->%v,cfg:%v,ex-cfg:%v", kv.me, kv.gid, kv.lastcfg.Num, kv.cfg.Num, kv.cfg, kv.lastcfg)
		return CommandResp{OK, ""}
	}
	return CommandResp{ErrOutdated, ""}
}

func (kv *ShardKV) getNoHandleG2s() map[int][]int {
	g2s := map[int][]int{}
	for shardId, gid := range kv.lastcfg.Shards {
		if gid == kv.gid && kv.cfg.Shards[shardId] != kv.gid && kv.stateMachine[shardId].Status == Pushing {
			newGid := kv.cfg.Shards[shardId]
			if newGid != 0 {
				g2s[newGid] = append(g2s[newGid], shardId)
			}
		}
	}
	return g2s
}

func (kv *ShardKV) pushShardsL() {
	g2sNeedPush := kv.getNoHandleG2s()
	DPrintf(DInfo, "S%v gid:%v need push:%v", kv.me, kv.gid, g2sNeedPush)
	wg := sync.WaitGroup{}
	for pullingGid, shardIds := range g2sNeedPush {
		wg.Add(1)
		go func(servers []string, cfgNum int, shardIds []int) {
			start := time.Now()
			defer wg.Done()
			index := 0
			for {
				shards := make(map[int]*Shard)
				for _, shardId := range shardIds {
					shards[shardId] = DeepCopy(kv.stateMachine[shardId]).(*Shard)
				}
				args := ShardArgs{CfgNum: cfgNum, Shards: shards}
				var resp ShardResp
				ok := kv.makeEnd(servers[index]).Call("ShardKV.PushShardsHandler", &args, &resp)
				if ok && (resp.Err == OK || time.Now().Sub(start) >= 500*time.Millisecond) {
					kv.rf.Start(Command{DeleteShards, ShardArgs{CfgNum: cfgNum, ShardIds: shardIds}})
					break
				}
				index = (index + 1) % len(servers)
			}
		}(kv.cfg.Groups[pullingGid], kv.cfg.Num, shardIds)
	}
	wg.Wait()
}

func (kv *ShardKV) PushShardsHandler(args *ShardArgs, resp *ShardResp) {
	kv.mu.Lock()
	if kv.cfg.Num < args.CfgNum {
		DPrintf(DServer, "S%v gid:%d no ready for [%v]", kv.me, kv.gid, args)
		kv.mu.Unlock()
		resp.Err = ErrNoReady
		return
	}
	if kv.cfg.Num > args.CfgNum {
		DPrintf(DServer, "S%v gid:%d received a duplicated [push cfg:%v while curr cfg:%v]", kv.me, kv.gid, args.CfgNum, kv.cfg.Num)
		kv.mu.Unlock()
		resp.Err = OK
		return
	}
	index, _, isLeader := kv.rf.Start(Command{AppendShards, *args})
	if !isLeader {
		kv.mu.Unlock()
		resp.Err = ErrWrongLeader
		return
	}
	ch := kv.getChL(index)
	kv.mu.Unlock()
	select {
	case <-time.After(1 * time.Second):
		kv.mu.Lock()
		DPrintf(DTrace, "S%v gid:%v -> TIMEOUT append:%v", kv.me, kv.gid, args.ShardIds)
		ok := true
		for _, shardId := range args.ShardIds {
			if kv.stateMachine[shardId].Status != Serving {
				ok = false
			}
		}
		if kv.cfg.Num > args.CfgNum || (kv.cfg.Num == args.CfgNum && ok) {
			resp.Err = OK
		} else {
			resp.Err = ErrTimeout
		}
		kv.mu.Unlock()
	case reply := <-ch:
		kv.mu.Lock()
		resp.Err = reply.Err
		DPrintf(DTrace, "S%v gid:%v -> OK append:%v", kv.me, kv.gid, args.ShardIds)
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) applyAppendShardsL(shardsInfo *ShardArgs) CommandResp {
	if shardsInfo.CfgNum == kv.cfg.Num {
		DPrintf(DApply, "S%v gid:%v applyAppendShards:[%v] when cfg:[%v]", kv.me, kv.gid, shardsInfo.Shards, kv.cfg)
		for shardId, shard := range shardsInfo.Shards {
			if kv.stateMachine[shardId].Log != nil && kv.stateMachine[shardId].Status != Pulling {
				continue
			}
			kv.stateMachine[shardId] = DeepCopy(shard).(*Shard)
			kv.stateMachine[shardId].Status = Serving
		}
		return CommandResp{OK, ""}
	} else if shardsInfo.CfgNum < kv.cfg.Num {
		return CommandResp{OK, ""}
	}
	DPrintf(DError, "S%v gid:%v rejects applyAppendShards cause [append cfg:%v while curr cfg:%v]", kv.me, kv.gid, shardsInfo.CfgNum, kv.cfg.Num)
	return CommandResp{ErrOutdated, ""}
}

func (kv *ShardKV) applyDeleteShardsL(shardsInfo *ShardArgs) CommandResp {
	if shardsInfo.CfgNum == kv.cfg.Num {
		DPrintf(DApply, "S%v gid:%v applyDeleteShards:[%v] when cfg:[%v]", kv.me, kv.gid, shardsInfo.ShardIds, kv.cfg)
		for _, shardId := range shardsInfo.ShardIds {
			kv.stateMachine[shardId] = &Shard{}
		}
		return CommandResp{OK, ""}
	} else if shardsInfo.CfgNum > kv.cfg.Num {
		DPrintf(DError, "S%v gid:%v rejects applyDeleteShards [delete cfg:%v while curr cfg:%v]", kv.me, kv.gid, shardsInfo.CfgNum, kv.cfg.Num)
		return CommandResp{ErrNoReady, ""}
	}
	return CommandResp{OK, ""}
}

func (kv *ShardKV) applyEmptyEntryL() CommandResp {
	return CommandResp{OK, ""}
}

func (kv *ShardKV) sendEmpty() {
	if !kv.rf.HasLogAtCurrentTerm() && !kv.killed() {
		kv.rf.Start(Command{EmptyEntry, nil})
	}
}

func (kv *ShardKV) serverable(shardId int) bool {
	return kv.cfg.Shards[shardId] == kv.gid && kv.stateMachine[shardId].Log != nil && kv.stateMachine[shardId].Status == Serving
}

func (kv *ShardKV) getEncodeStateL() []byte {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.stateMachine)
	e.Encode(kv.lastApplied)
	e.Encode(kv.lastcfg)
	e.Encode(kv.cfg)
	return w.Bytes()
}

func (kv *ShardKV) readPersistL(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	DPrintf(DDrop, "S%v gid:%v readPersist", kv.me, kv.gid)
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	var lastApplied int
	var stateMachine map[int]*Shard
	var lastcfg shardctrler.Config
	var cfg shardctrler.Config
	if d.Decode(&stateMachine) != nil ||
		d.Decode(&lastApplied) != nil ||
		d.Decode(&lastcfg) != nil ||
		d.Decode(&cfg) != nil {
		DPrintf(DWarn, "decode err!")
	} else {
		kv.stateMachine = stateMachine
		kv.lastApplied = lastApplied
		kv.lastIncludeIndex = lastApplied
		kv.lastcfg = lastcfg
		kv.cfg = cfg
	}
	DPrintf(DError, "S%v gid:%d install snapshot cfg[%v]", kv.me, kv.gid, kv.cfg)
}

func (kv *ShardKV) snapshotL(index int) {
	snapshot := kv.getEncodeStateL()
	kv.lastIncludeIndex = index
	kv.rf.Snapshot(index, snapshot)
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	atomic.StoreInt32(&kv.dead, 1)
	DPrintf(DDrop, "S%v gid:%v dead", kv.me, kv.gid)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) daemon(f func(), duration time.Duration) {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			f()
		}
		time.Sleep(duration)
	}
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// stateMachine. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// makeEnd(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and makeEnd() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, makeEnd func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Command{})
	gob.Register(CommandArgs{})
	gob.Register(CommandResp{})
	gob.Register(ShardArgs{})
	gob.Register(ShardResp{})
	gob.Register(shardctrler.Config{})

	kv := &ShardKV{
		mu:               sync.Mutex{},
		me:               me,
		rf:               nil,
		applyCh:          make(chan raft.ApplyMsg),
		makeEnd:          makeEnd,
		gid:              gid,
		ctrlers:          ctrlers,
		maxraftstate:     maxraftstate,
		lastcfg:          shardctrler.Config{},
		cfg:              shardctrler.Config{},
		sc:               shardctrler.MakeClerk(ctrlers),
		dead:             0,
		lastApplied:      0,
		lastIncludeIndex: 0,
		stateMachine:     make(map[int]*Shard),
		commandReplyCh:   make(map[int]chan *CommandResp),
	}
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	for i := 0; i < shardctrler.NShards; i++ {
		kv.stateMachine[i] = &Shard{Status: Serving}
	}
	DPrintf(DDrop, "S%v gid:%v start", kv.me, kv.gid)

	kv.readPersistL(persister.ReadSnapshot())
	go kv.applier()
	go kv.daemon(kv.updateCfg, updateCfgTimeout)
	go kv.daemon(kv.sendEmpty, sendEmptyTimeout)
	return kv
}
