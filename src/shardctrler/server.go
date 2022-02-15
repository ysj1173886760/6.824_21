package shardctrler


import "6.824/raft"
import "6.824/labrpc"
import "sync"
import "6.824/labgob"
import "sort"
import "log"
import "time"
import "sync/atomic"

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead	int32

	// Your data here.

	configs []Config // indexed by config num

	// clientID -> seqID
	clientSeq map[int64]int
	// index -> chan
	commitChannel map[int]chan Op
}

type Pair struct {
	GID int
	Num int
}

type PairList []Pair

func (p PairList) Len() int {
	return len(p)
}

func (p PairList) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p PairList) Less(i, j int) bool {
	if p[i].Num == p[j].Num {
		return p[i].GID < p[j].GID
	}
	return p[i].Num > p[j].Num
}


type Op struct {
	// Your data here.
	Type string
	ServerID int
	ClientID int64
	SeqID int
	// JoinArgs
	Servers map[int][]string
	// LeaveArgs
	GIDs []int
	// MoveArgs
	Shard int
	GID int
	// QueryArgs
	Num int
	// QueryReply
	Config Config
}

const (
	JOIN = "Join"
	LEAVE = "Leave"
	MOVE = "Move"
	QUERY = "Query"
)

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	command := Op {
		Type: JOIN,
		ClientID: args.ClientID,
		SeqID: args.SeqID,
		ServerID: sc.me,
		Servers: args.Servers,
	}

	sc.mu.Lock()
	index, _, isLeader := sc.rf.Start(command)

	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	DPrintf("[%d] start Join Command, ClientID=%d SeqID=%d", sc.me, args.ClientID, args.SeqID)

	ch := sc.GetChannelL(index)
	sc.mu.Unlock()

	select {
	case op := <- ch:
		if args.ClientID != op.ClientID || args.SeqID != op.SeqID {
			reply.WrongLeader = true
		} else {
			reply.Err = OK
		}
	case <- time.After(1 * time.Second):
		reply.Err = ErrTimeout
	}

	sc.mu.Lock()
	delete(sc.commitChannel, index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	command := Op {
		Type: LEAVE,
		GIDs: args.GIDs,
		ServerID: sc.me,
		ClientID: args.ClientID,
		SeqID: args.SeqID,
	}

	sc.mu.Lock()
	index, _, isLeader := sc.rf.Start(command)

	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	DPrintf("[%d] start Leave Command, ClientID=%d SeqID=%d", sc.me, args.ClientID, args.SeqID)

	ch := sc.GetChannelL(index)
	sc.mu.Unlock()

	select {
	case op := <- ch:
		if args.ClientID != op.ClientID || args.SeqID != op.SeqID {
			reply.WrongLeader = true
		} else {
			reply.Err = OK
		}
	case <- time.After(1 * time.Second):
		reply.Err = ErrTimeout
	}

	sc.mu.Lock()
	delete(sc.commitChannel, index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	command := Op {
		Type: MOVE,
		ClientID: args.ClientID,
		SeqID: args.SeqID,
		ServerID: sc.me,
		GID: args.GID,
		Shard: args.Shard,
	}

	sc.mu.Lock()
	index, _, isLeader := sc.rf.Start(command)

	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	DPrintf("[%d] start Move Command, ClientID=%d SeqID=%d", sc.me, args.ClientID, args.SeqID)

	ch := sc.GetChannelL(index)
	sc.mu.Unlock()

	select {
	case op := <- ch:
		if args.ClientID != op.ClientID || args.SeqID != op.SeqID {
			reply.WrongLeader = true
		} else {
			reply.Err = OK
		}
	case <- time.After(1 * time.Second):
		reply.Err = ErrTimeout
	}

	sc.mu.Lock()
	delete(sc.commitChannel, index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	command := Op {
		Type: QUERY,
		ClientID: args.ClientID,
		SeqID: args.SeqID,
		ServerID: sc.me,
		Num: args.Num,
	}

	sc.mu.Lock()
	index, _, isLeader := sc.rf.Start(command)

	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	DPrintf("[%d] start Query Command, ClientID=%d SeqID=%d", sc.me, args.ClientID, args.SeqID)

	ch := sc.GetChannelL(index)
	sc.mu.Unlock()

	select {
	case op := <- ch:
		if args.ClientID != op.ClientID || args.SeqID != op.SeqID {
			reply.WrongLeader = true
		} else {
			reply.Err = OK
			reply.Config = op.Config
			DPrintf("[%d] return config %v", sc.me, op.Config)
		}
	case <- time.After(1 * time.Second):
		reply.Err = ErrTimeout
	}

	sc.mu.Lock()
	delete(sc.commitChannel, index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) executor() {
	for sc.killed() == false {
		msg := <- sc.applyCh

		if msg.CommandValid {
			sc.mu.Lock()
			command := msg.Command.(Op)
			ch, exists := sc.commitChannel[msg.CommandIndex]
			switch command.Type {
			case JOIN:
				if sc.clientSeq[command.ClientID] < command.SeqID {
					sc.processJoinL(&command)
				}
			case LEAVE:
				if sc.clientSeq[command.ClientID] < command.SeqID {
					sc.processLeaveL(&command)
				}
			case MOVE:
				if sc.clientSeq[command.ClientID] < command.SeqID {
					sc.processMoveL(&command)
				}
			case QUERY:
				sc.processQueryL(&command)
			}

			if sc.clientSeq[command.ClientID] < command.SeqID {
				sc.clientSeq[command.ClientID] = command.SeqID
			}

			sc.mu.Unlock()

			if command.ServerID == sc.me && exists {
				DPrintf("[%d] Send Op though apply channel index=%d ClientID=%d SeqID=%d", sc.me, msg.CommandIndex, command.ClientID, command.SeqID)
				ch <- command
			}
		}
	}
}

func (sc *ShardCtrler) processJoinL(op *Op) {
	sc.generateNewConfigL()
	appendingList := []int{}
	for GID, _:= range op.Servers {
		appendingList = append(appendingList, GID)
	}
	sort.Ints(appendingList)

	for _, GID:= range appendingList {
		servers := op.Servers[GID]
		lastConfig := &sc.configs[len(sc.configs) - 1]

		// do the rebalancing
		previousNum := len(lastConfig.Groups)
		numGroups := previousNum + 1

		if previousNum == 0 {
			for i := 0; i < NShards; i++ {
				lastConfig.Shards[i] = GID
			}
		} else {
			count := NShards / numGroups
			allocateMap := map[int][]int{}
			for i := 0; i < NShards; i++ {
				gid := lastConfig.Shards[i]
				allocateMap[gid] = append(allocateMap[gid], i)
			}
			
			existingList := PairList{}
			for k, v := range allocateMap {
				existingList = append(existingList, Pair{k, len(v)})
			}

			for i := 0; i < count; i++ {
				sort.Sort(existingList)
				gid := existingList[0].GID
				l := len(allocateMap[gid])
				shardID := allocateMap[gid][l - 1]
				// remove the last shard
				allocateMap[gid] = allocateMap[gid][: l - 1]
				// append it to the new group
				allocateMap[GID] = append(allocateMap[GID], shardID)
				// decrease the num
				existingList[0].Num--
			}
			for k, v := range allocateMap {
				for _, shardID := range v {
					lastConfig.Shards[shardID] = k
				}
			}
		}

		lastConfig.Groups[GID] = servers
	}
	DPrintf("[%d] currentConfiguration %v", sc.me, sc.configs[len(sc.configs) - 1])
}

func (sc *ShardCtrler) processLeaveL(op *Op) {
	sc.generateNewConfigL()
	lastConfig := &sc.configs[len(sc.configs) - 1]

	// do the rebalancing

	for _, gid := range op.GIDs {
		delete(lastConfig.Groups, gid)
	}

	allocateMap := map[int][]int{}
	rebalanceList := []int{}

	for k, _ := range lastConfig.Groups {
		allocateMap[k] = []int{}
	}

	for i := 0; i < NShards; i++ {
		gid := lastConfig.Shards[i]
		_, exists := lastConfig.Groups[gid]
		// shard i need to rebalance
		if !exists {
			lastConfig.Shards[i] = 0;
			rebalanceList = append(rebalanceList, i)
		} else {
			allocateMap[gid] = append(allocateMap[gid], i)
		}
	}

	existingList := PairList{}
	for k, v := range allocateMap {
		existingList = append(existingList, Pair{k, len(v)})
	}

	numExist := len(existingList)
	if numExist == 0 {
		lastConfig.Groups = map[int][]string{}
		return
	}

	for i := range rebalanceList {
		sort.Sort(existingList)
		gid := existingList[len(existingList) - 1].GID
		allocateMap[gid] = append(allocateMap[gid], rebalanceList[i])
		existingList[len(existingList) - 1].Num++
	}

	for k, v := range allocateMap {
		for _, shardID := range v {
			lastConfig.Shards[shardID] = k
		}
	}
	DPrintf("[%d] currentConfiguration %v", sc.me, lastConfig)
}

func (sc *ShardCtrler) processMoveL(op *Op) {
	sc.generateNewConfigL()
	lastConfig := &sc.configs[len(sc.configs) - 1]
	lastConfig.Shards[op.Shard] = op.GID
	DPrintf("[%d] currentConfiguration %v", sc.me, lastConfig)
}

func (sc *ShardCtrler) processQueryL(op *Op) {
	var originConfig *Config
	if op.Num == -1 || op.Num >= len(sc.configs) {
		originConfig = &sc.configs[len(sc.configs) - 1]
	} else {
		originConfig = &sc.configs[op.Num]
	}

	new_config := Config{}
	new_config.Num = originConfig.Num
	new_config.Shards = originConfig.Shards
	mp := map[int][]string{}
	for k, v := range originConfig.Groups {
		mp[k] = v
	}
	new_config.Groups = mp
	op.Config = new_config
}

func (sc *ShardCtrler) GetChannelL(index int) chan Op {
	ch, exists := sc.commitChannel[index]
	if !exists {
		ch = make(chan Op, 1)
		sc.commitChannel[index] = ch
	}
	return ch
}

func (sc *ShardCtrler) generateNewConfigL() {
	new_config := Config{}
	new_config.Num = len(sc.configs)
	new_config.Shards = sc.configs[new_config.Num - 1].Shards
	mp := map[int][]string{}
	for k, v := range sc.configs[new_config.Num - 1].Groups {
		mp[k] = v
	}
	new_config.Groups = mp
	sc.configs = append(sc.configs, new_config)
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me
	sc.dead = 0

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.clientSeq = make(map[int64]int)
	sc.commitChannel = make(map[int]chan Op)

	go sc.executor()

	return sc
}
