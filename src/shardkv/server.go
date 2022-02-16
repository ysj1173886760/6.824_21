package shardkv


import "6.824/labrpc"
import "6.824/raft"
import "sync"
import "6.824/labgob"
import "6.824/shardctrler"
import "log"
import "sync/atomic"
import "time"
import "bytes"

const Debug = true

const CommonInterval = 10
const ConfigPullInterval = 100

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	PUT		= "Put"
	APPEND	= "Append"
	GET		= "Get"
)

const (
	OPERATION 	= "Operation"
	CONFIG 		= "Config"
)

const (
	VALID 			= "Valid"
	INVALID 		= "Invalid"
	REQUESTDATA 	= "RequestData"
	WAITREQUEST 	= "WaitRequest"
	NOTIFYDELETE 	= "NotifyDelete"
)

type Op struct {
	Type    string
	Value	string
	Key		string

	// who is response to respond this request
	KvID 		int
	ClientID	int64
	// requestID
	SeqID		int
}

type ConfigLog struct {
	Config	shardctrler.Config
}

type Command struct {
	Type 	string
	Data	interface{}
}

type Shard struct {
	db				map[string]string
	clientSeq		map[int64]int
	state 			string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	dead		 int32
	
	// shardController
	sc      	*shardctrler.Clerk

	shard				[shardctrler.NShards]Shard
	// index -> chan
	commitChannel		map[int]chan Op
	// commit index used in snapshot
	commitIndex 		int
	// currentConfig
	curConfig 		shardctrler.Config
	// previousConfig
	preConfig 	shardctrler.Config
}

func (kv *ShardKV) startNewConfig(config *shardctrler.Config) {
	log := *config
	command := Command{ Type: CONFIG, Data: ConfigLog{log} }
	kv.rf.Start(command)
}

func (kv *ShardKV) configPuller() {
	for kv.killed() == false {
		time.Sleep(time.Millisecond * time.Duration(ConfigPullInterval))
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			continue
		}
		kv.mu.Lock()
		currentConfigNum := kv.curConfig.Num
		kv.mu.Unlock()

		config := kv.sc.Query(currentConfigNum + 1)
		if config.Num == currentConfigNum + 1 {
			// we commit this config
			kv.startNewConfig(&config)
			DPrintf("[%d-%d] pulled new config %v start to commit", kv.gid, kv.me, config)
		}

	}
}

func (kv *ShardKV) applyOperation(msg *raft.ApplyMsg) {
	kv.mu.Lock()
	ch, exists := kv.commitChannel[msg.CommandIndex]
	C := msg.Command.(Command)
	command := C.Data.(Op)
	shard := key2shard(command.Key)

	switch command.Type {
	case GET:
		value, appear := kv.shard[shard].db[command.Key]
		// check whether i'm responsable to send the command back to channel
		if command.KvID == kv.me && exists {
			if !appear {
				command.Value = ""
			} else {
				command.Value = value
			}
		}
	case APPEND:
		value, appear := kv.shard[shard].db[command.Key]
		// first check whether we have applied this command
		if kv.shard[shard].clientSeq[command.ClientID] < command.SeqID {
			if !appear {
				kv.shard[shard].db[command.Key] = command.Value
			} else {
				kv.shard[shard].db[command.Key] = value + command.Value
			}
		}
		
	case PUT:
		if kv.shard[shard].clientSeq[command.ClientID] < command.SeqID {
			kv.shard[shard].db[command.Key] = command.Value
		}
	}
	// update commit Index
	kv.commitIndex = msg.CommandIndex

	// update the seqID to prevent duplicated operation
	if kv.shard[shard].clientSeq[command.ClientID] < command.SeqID {
		kv.shard[shard].clientSeq[command.ClientID] = command.SeqID
	}

	kv.mu.Unlock()

	if command.KvID == kv.me && exists {
		DPrintf("[%d-%d] Send Op though apply channel index=%d ClientID=%d SeqID=%d", kv.gid, kv.me, msg.CommandIndex, command.ClientID, command.SeqID)
		ch <- command
	}
}

func (kv *ShardKV) applyConfig(msg *raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	C := msg.Command.(Command)
	configLog := C.Data.(ConfigLog)
	if kv.curConfig.Num >= configLog.Config.Num {
		return
	}
	kv.preConfig = kv.curConfig
	kv.curConfig = configLog.Config
	DPrintf("[%d-%d] commit new config %v", kv.gid, kv.me, kv.curConfig)
}

func (kv *ShardKV) executor() {
	for kv.killed() == false {
		msg := <- kv.applyCh


		if msg.CommandValid {
			command := msg.Command.(Command)
			switch command.Type {
			case OPERATION:
				kv.applyOperation(&msg)
			case CONFIG:
				kv.applyConfig(&msg)
			}
		} else if msg.SnapshotValid {
			go kv.applySnapshot(msg.Snapshot, msg.SnapshotTerm, msg.SnapshotIndex)
		}
	}
}

func (kv *ShardKV) checkShardKeyL(key string) bool {
	shard := key2shard(key)
	return kv.curConfig.Shards[shard] == kv.gid
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	command := Op {
		Type: GET,
		Key: args.Key,
		KvID: kv.me,
		ClientID: args.ClientID,
		SeqID: args.SeqID,
	}

	kv.mu.Lock()

	if kv.checkShardKeyL(args.Key) == false {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	shard := key2shard(args.Key)
	if kv.shard[shard].state == REQUESTDATA {
		// although i'm responseable for this data, but i need some time to request the data, so please wait for a second
		reply.Err = ErrTryAgain
		kv.mu.Unlock()
		return
	}

	if kv.shard[shard].clientSeq[args.ClientID] >= args.SeqID {
		reply.Err = OK
		reply.Value = kv.shard[shard].db[args.Key]
		kv.mu.Unlock()
		return
	}

	index, term, isLeader := kv.rf.Start(Command{ Type: OPERATION, Data: command })

	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	DPrintf("[%d-%d] start Get command, ClientID=%d SeqID=%d Key=%v", kv.gid, kv.me, args.ClientID, args.SeqID, args.Key)

	// construct the channel and wait on the channel
	ch := kv.GetChannelL(index)
	kv.mu.Unlock()

	select {
	case op := <- ch:
		if op.ClientID != args.ClientID || op.SeqID != args.SeqID {
			reply.Err = ErrWrongLeader
		} else {
			reply.Value = op.Value
			reply.Err = OK
		}
		DPrintf("[%d-%d] received reply index=%d ClientID=%d SeqID=%d", kv.gid, kv.me, index, op.ClientID, op.SeqID)
	case <- time.After(1 * time.Second):
		reply.Err = ErrTimeout
	}

	kv.mu.Lock()
	delete(kv.commitChannel, index)
	kv.mu.Unlock()

	// check Am I still the leader
	new_term, new_leader := kv.rf.GetState()
	if new_term != term || new_leader == false {
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *ShardKV) GetChannelL(index int) chan Op {
	ch, exists := kv.commitChannel[index]
	if !exists {
		ch = make(chan Op, 1)
		kv.commitChannel[index] = ch
	}
	return ch
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	command := Op {
		Type: args.Op,
		Key: args.Key,
		Value: args.Value,
		KvID: kv.me,
		ClientID: args.ClientID,
		SeqID: args.SeqID,
	}

	kv.mu.Lock()

	if kv.checkShardKeyL(args.Key) == false {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	shard := key2shard(args.Key)

	if kv.shard[shard].state == REQUESTDATA {
		reply.Err = ErrTryAgain
		kv.mu.Unlock()
		return
	}

	if kv.shard[shard].clientSeq[args.ClientID] >= args.SeqID {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	index, term, isLeader := kv.rf.Start(Command{ Type: OPERATION, Data: command })
	// kv.timeMap[index] = time.Now()

	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("[%d-%d] start PutAppend command, ClientID=%d SeqID=%d Key=%v Value=%v", kv.gid, kv.me, args.ClientID, args.SeqID, args.Key, args.Value)

	// construct the channel and wait on the channel
	// make Start and Construct channel atomic
	ch := kv.GetChannelL(index)
	kv.mu.Unlock()

	select {
	case op := <- ch:
		// sanity check here
		if op.ClientID != args.ClientID || op.SeqID != args.SeqID {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
		}
		DPrintf("[%d-%d] received reply index=%d ClientID=%d SeqID=%d", kv.gid, kv.me, index, op.ClientID, op.SeqID)
	case <- time.After(1 * time.Second):
		reply.Err = ErrTimeout
	}

	// delete channel
	kv.mu.Lock()
	delete(kv.commitChannel, index)
	kv.mu.Unlock()

	// check Am I still the leader
	new_term, new_leader := kv.rf.GetState()
	if new_term != term || new_leader == false {
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *ShardKV) snapshotThread() {
	if kv.maxraftstate == -1 {
		return
	}

	threshold := int(float64(kv.maxraftstate) * 0.9)
	for kv.killed() == false {
		interval := time.Duration(CommonInterval)
		time.Sleep(time.Millisecond * interval)
		
		if kv.rf.RaftStateSize() > threshold {
			// start snapshot
			kv.mu.Lock()
			DPrintf("[%d-%d] startSnapshot index=%d", kv.gid, kv.me, kv.commitIndex)

			// if we can do COW would be greater
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.shard)
			e.Encode(kv.commitIndex)
			commitIndex := kv.commitIndex

			kv.mu.Unlock()

			data := w.Bytes()
			kv.rf.Snapshot(commitIndex, data)
		}

	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) readPersist() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	snapshot := kv.rf.ReadSnapshot()
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	
	var shard [shardctrler.NShards]Shard
	var commitIndex int

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	if d.Decode(&shard) != nil ||
	   d.Decode(&commitIndex) != nil {
		DPrintf("[%d-%d] failed to read from persist", kv.gid, kv.me)
	} else {
		kv.shard = shard
		kv.commitIndex = commitIndex
		DPrintf("[%d-%d] recover from persist, commitIndex=%d", kv.gid, kv.me, kv.commitIndex)
	}
}

func (kv *ShardKV) applySnapshot(snapshot []byte, snapshotTerm, snapshotIndex int) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	var shard [shardctrler.NShards]Shard
	var commitIndex int

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	if d.Decode(&shard) != nil ||
	   d.Decode(&commitIndex) != nil {
		DPrintf("[%d-%d] failed to read from snapshot", kv.gid, kv.me)
	} else {
		if kv.rf.CondInstallSnapshot(snapshotTerm, snapshotIndex, snapshot) {
			kv.mu.Lock()
			kv.shard = shard
			kv.commitIndex = commitIndex
			DPrintf("[%d-%d] apply snapshot term=%d index=%d commitIndex=%d", kv.gid, kv.me, snapshotTerm, snapshotIndex, kv.commitIndex)
			kv.mu.Unlock()
		} else {
			DPrintf("[%d-%d] failed to apply snapshot term=%d index=%d", kv.gid, kv.me, snapshotTerm, snapshotIndex)
		}
	}
}


//
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
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(Command{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(ConfigLog{})

	kv := new(ShardKV)
	kv.me = me
	kv.dead = 0
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.commitChannel = make(map[int]chan Op)
	kv.commitIndex = 0
	kv.sc = shardctrler.MakeClerk(ctrlers)

	for i := range kv.shard {
		kv.shard[i].db = make(map[string]string)
		kv.shard[i].clientSeq = make(map[int64]int)
		kv.shard[i].state = INVALID
	}

	kv.readPersist()

	go kv.executor()
	go kv.snapshotThread()
	go kv.configPuller()

	return kv
}
