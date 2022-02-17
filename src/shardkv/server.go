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
import "fmt"

const Debug = false

const CommonInterval = 10
const ConfigPullInterval = 100
const DataPullInterval = 200
const DataDeleteInterval = 200
const LogCheckingInterval = 300

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
	OPERATION 		= "Operation"
	CONFIG 			= "Config"
	SHARDDATA		= "ShardData"
	DELETEDATA  	= "DeleteData"
	EMPTY			= "Empty"
	CONFIRMDELETE 	= "ConfirmDelete"
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

	// for rechecking
	Err			Err
	ConfigNum   int

	// who is response to respond this request
	KvID 		int
	ClientID	int64
	// requestID
	SeqID		int
}

type EmptyLog struct {

}

type ConfirmLog struct {
	ConfigNum	int
	ShardID 	int
}

type ConfigLog struct {
	Config	shardctrler.Config
}

type ShardDataLog struct {
	ConfigNum 	int
	ShardID		int
	DB 			map[string]string
	ClientSeq	map[int64]int
}

type DeleteDataLog struct {
	ConfigNum 	int
	ShardID 	int
}

type Command struct {
	Type 	string
	Data	interface{}
}

type Shard struct {
	DB				map[string]string
	ClientSeq		map[int64]int
	State 			string
}

func (s Shard) String() string {
	return fmt.Sprintf("%v", s.State)
}

type ShardKV struct {
	mu           sync.RWMutex
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

// RPC related

func (kv *ShardKV) RequestData(args *RequestDataArgs, reply *RequestDataReply) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	reply.GID = kv.gid
	if args.ConfigNum != kv.curConfig.Num {
		DPrintf("[%d-%d] reject request data from %d due to unmatched ConfigNum %d", kv.gid, kv.me, args.GID, kv.curConfig.Num)
		reply.Valid = false
		return
	}

	// sanity check, normally this won't ever happen
	if kv.shard[args.ShardID].State != WAITREQUEST {
		DPrintf("[%d-%d] reject request data from %d due to unmatched state %v", kv.gid, kv.me, args.GID, kv.shard[args.ShardID].State)
		reply.Valid = false
		return
	}

	// return the data
	reply.Valid = true
	reply.DB, reply.ClientSeq = kv.MakeShardL(args.ShardID)
	DPrintf("[%d-%d] return data for %d", kv.gid, kv.me, args.GID)
}

// args is for checking assumption
func (kv *ShardKV) sendRequestData(ShardID, ConfigNum int) {
	kv.mu.RLock()

	if ConfigNum != kv.curConfig.Num {
		// DPrintf("[%d-%d] return due to unmatch configNum %d %d", kv.gid, kv.me, kv.curConfig.Num, ConfigNum)
		kv.mu.RUnlock()
		return
	}

	// means that data has already been pulled
	if kv.shard[ShardID].State != REQUESTDATA {
		// DPrintf("[%d-%d] return due to unmatch state %v", kv.gid, kv.me, kv.curConfig)
		kv.mu.RUnlock()
		return
	}

	args := RequestDataArgs{}
	args.GID = kv.gid
	args.ShardID = ShardID
	args.ConfigNum = ConfigNum

	to_gid := kv.preConfig.Shards[ShardID]
	servers := kv.preConfig.Groups[to_gid]

	kv.mu.RUnlock()

	for si := 0; si < len(servers); si++ {
		server := kv.make_end(servers[si])

		var reply RequestDataReply
		ok := server.Call("ShardKV.RequestData", &args, &reply)
		DPrintf("[%d-%d] send RequestData RPC args %v reply %v", kv.gid, kv.me, args, reply)
	
		if !ok {
			continue
		}

		if !reply.Valid {
			continue
		}

		// if the data is valid, then we need to start a new command to commit this data
		// hold on, before that, recheck the assumption
		kv.mu.RLock()
		if ConfigNum != kv.curConfig.Num || kv.shard[args.ShardID].State != REQUESTDATA {
			kv.mu.RUnlock()
			return
		}

		DPrintf("[%d-%d] received data from %d, start command", kv.gid, kv.me, reply.GID)
		log := ShardDataLog{ ConfigNum, ShardID, reply.DB, reply.ClientSeq }
		command := Command{ SHARDDATA, log }
		kv.rf.Start(command)
		// whether we are leader or not, we are going to exit this function. So we don't have to recheck it

		kv.mu.RUnlock()
		break
	}

}

func (kv *ShardKV) dataPuller() {
	for kv.killed() == false {
		time.Sleep(time.Millisecond * time.Duration(DataPullInterval))

		DPrintf("[%d-%d] dataPuller still working", kv.gid, kv.me)
		kv.mu.RLock()
		for shardID := 0; shardID < shardctrler.NShards; shardID++ {
			if kv.shard[shardID].State == REQUESTDATA {
				go kv.sendRequestData(shardID, kv.curConfig.Num)
			}
		}
		kv.mu.RUnlock()
	}
}

func (kv *ShardKV) MakeShardL(shardID int) (map[string]string, map[int64]int) {
	db := map[string]string{}
	clientSeq := map[int64]int{}

	for k, v := range kv.shard[shardID].DB {
		db[k] = v
	}
	for k, v := range kv.shard[shardID].ClientSeq {
		clientSeq[k] = v
	}

	return db, clientSeq
}

func DuplicateDB(mp map[string]string) map[string]string {
	db := map[string]string{}
	for k, v := range mp {
		db[k] = v
	}
	return db
}

func DuplicateClientSeq(mp map[int64]int) map[int64]int {
	clientSeq := map[int64]int{}
	for k, v := range mp {
		clientSeq[k] = v
	}
	return clientSeq
}

func (kv *ShardKV) RequestDelete(args *RequestDeleteArgs, reply *RequestDeleteReply) {
	// only leader can delete data
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		reply.Deleted = false
		return
	}

	kv.mu.RLock()
	defer kv.mu.RUnlock()

	DPrintf("[%d-%d] receive requestDelete RPC from %d shard=%d configNum=%d", kv.gid, kv.me, args.GID, args.ShardID, args.ConfigNum)
	reply.GID = kv.gid
	if args.ConfigNum < kv.curConfig.Num {
		// if the config has already move on, means we have deleted this data
		reply.Deleted = true
		return
	} else if args.ConfigNum > kv.curConfig.Num {
		reply.Deleted = false
		return
	}

	if kv.shard[args.ShardID].State != WAITREQUEST {
		// already deleted
		reply.Deleted = true
		return
	}

	// start to commit a delete log
	log := DeleteDataLog{ kv.curConfig.Num, args.ShardID }
	command := Command{ DELETEDATA, log }
	_, _, isLeader = kv.rf.Start(command)

	if !isLeader {
		reply.WrongLeader = true
		reply.Deleted = false
		return
	} else {
		DPrintf("[%d-%d] start delete shard %d", kv.gid, kv.me, args.ShardID)
	}
}

// args is for checking assumption
func (kv *ShardKV) sendDeleteRequest(ShardID, ConfigNum int) {
	kv.mu.RLock()

	if ConfigNum != kv.curConfig.Num {
		// DPrintf("[%d-%d] return due to unmatch configNum %d %d", kv.gid, kv.me, kv.curConfig.Num, ConfigNum)
		kv.mu.RUnlock()
		return
	}

	// means that data has already been pulled
	if kv.shard[ShardID].State != NOTIFYDELETE {
		// DPrintf("[%d-%d] return due to unmatch state %v", kv.gid, kv.me, kv.curConfig)
		kv.mu.RUnlock()
		return
	}

	args := RequestDeleteArgs{}
	args.GID = kv.gid
	args.ShardID = ShardID
	args.ConfigNum = ConfigNum

	to_gid := kv.preConfig.Shards[ShardID]
	servers := kv.preConfig.Groups[to_gid]

	kv.mu.RUnlock()

	for si := 0; si < len(servers); si++ {
		server := kv.make_end(servers[si])

		var reply RequestDeleteReply
		ok := server.Call("ShardKV.RequestDelete", &args, &reply)
		DPrintf("[%d-%d] send RequestDelete RPC args %v reply %v", kv.gid, kv.me, args, reply)
	
		if !ok {
			continue
		}

		if reply.WrongLeader {
			continue
		}

		kv.mu.RLock()
		// recheck assumption
		if ConfigNum != kv.curConfig.Num || kv.shard[args.ShardID].State != NOTIFYDELETE {
			kv.mu.RUnlock()
			return
		}

		if reply.Deleted {
			// start a command to notify other followers that data has been deleted
			log := ConfirmLog{ kv.curConfig.Num, args.ShardID }
			kv.rf.Start(Command{ CONFIRMDELETE, log })
			DPrintf("[%d-%d] start confirm delete %d", kv.gid, kv.me, args.ShardID)
		}

		kv.mu.RUnlock()
		break
	}

}

func (kv *ShardKV) dataDeleter() {
	for kv.killed() == false {
		time.Sleep(time.Millisecond * time.Duration(DataDeleteInterval))
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			continue
		}

		DPrintf("[%d-%d] dataDeleter still working", kv.gid, kv.me)
		kv.mu.RLock()
		for shardID := 0; shardID < shardctrler.NShards; shardID++ {
			if kv.shard[shardID].State == NOTIFYDELETE {
				DPrintf("[%d-%d] detect %d need to delete, spawn goroutine configNum %d", kv.gid, kv.me, shardID, kv.curConfig.Num)
				go kv.sendDeleteRequest(shardID, kv.curConfig.Num)
			}
		}
		kv.mu.RUnlock()
	}
}

func (kv *ShardKV) startNewConfig(config *shardctrler.Config) {
	log := *config
	command := Command{ Type: CONFIG, Data: ConfigLog{log} }
	kv.rf.Start(command)
}

func (kv *ShardKV) checkCanPullConfigL() bool {
	for shardID := 0; shardID < shardctrler.NShards; shardID++ {
		// during the config
		if kv.shard[shardID].State != INVALID && kv.shard[shardID].State != VALID {
			return false
		}
	}
	return true
}

func (kv *ShardKV) configPuller() {
	for kv.killed() == false {
		time.Sleep(time.Millisecond * time.Duration(ConfigPullInterval))
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			continue
		}
		kv.mu.RLock()
		currentConfigNum := kv.curConfig.Num
		shouldPullConfig := kv.checkCanPullConfigL()
		kv.mu.RUnlock()

		if shouldPullConfig {
			config := kv.sc.Query(currentConfigNum + 1)
			if config.Num == currentConfigNum + 1 {
				// we commit this config
				kv.startNewConfig(&config)
				DPrintf("[%d-%d] pulled new config %v start to commit", kv.gid, kv.me, config)
			}
		}

	}
}

func (kv *ShardKV) logChecker() {
	for kv.killed() == false {
		time.Sleep(time.Millisecond * time.Duration(LogCheckingInterval))
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			continue
		}

		hasLog := kv.rf.CheckLogTerm()
		if !hasLog {
			log := EmptyLog{}
			kv.rf.Start(Command{ EMPTY, log })
			DPrintf("[%d-%d] start a empty log", kv.gid, kv.me)
		}
	}
}

func (kv *ShardKV) applyOperation(msg *raft.ApplyMsg) {
	kv.mu.Lock()
	ch, exists := kv.commitChannel[msg.CommandIndex]
	C := msg.Command.(Command)
	command := C.Data.(Op)
	shard := key2shard(command.Key)

	if kv.curConfig.Num != command.ConfigNum {
		command.Err = ErrTryAgain
	} else if kv.shard[shard].State == INVALID || kv.shard[shard].State == WAITREQUEST {
		// i'm no longer hold the data
		command.Err = ErrWrongGroup
	} else {
		command.Err = OK
		switch command.Type {
		case GET:
			value, appear := kv.shard[shard].DB[command.Key]
			// check whether i'm responsable to send the command back to channel
			if command.KvID == kv.me && exists {
				if !appear {
					command.Value = ""
				} else {
					command.Value = value
				}
			}
		case APPEND:
			value, appear := kv.shard[shard].DB[command.Key]
			// first check whether we have applied this command
			if kv.shard[shard].ClientSeq[command.ClientID] < command.SeqID {
				if !appear {
					kv.shard[shard].DB[command.Key] = command.Value
				} else {
					kv.shard[shard].DB[command.Key] = value + command.Value
				}
			}
			
		case PUT:
			if kv.shard[shard].ClientSeq[command.ClientID] < command.SeqID {
				kv.shard[shard].DB[command.Key] = command.Value
			}
		}

		// update the seqID to prevent duplicated operation
		if kv.shard[shard].ClientSeq[command.ClientID] < command.SeqID {
			kv.shard[shard].ClientSeq[command.ClientID] = command.SeqID
		}
	}

	kv.mu.Unlock()

	if command.KvID == kv.me && exists {
		DPrintf("[%d-%d] Send Op though apply channel index=%d ClientID=%d SeqID=%d", kv.gid, kv.me, msg.CommandIndex, command.ClientID, command.SeqID)
		ch <- command
	}
}

func (kv *ShardKV) updateShardStateL() {
	for shardID := 0; shardID < shardctrler.NShards; shardID++ {
		// if it previously belong to me
		if kv.preConfig.Shards[shardID] == kv.gid {
			if kv.curConfig.Shards[shardID] == 0 {
				// if no one is response for this shard, maybe we can discard it?
				kv.shard[shardID].State = INVALID
			} else if kv.curConfig.Shards[shardID] != kv.gid {
				// if it doesn't belong to me, set it to WaitRequest state
				kv.shard[shardID].State = WAITREQUEST
			}
		}
		// if it currently belong to me
		if kv.curConfig.Shards[shardID] == kv.gid {
			if kv.preConfig.Shards[shardID] == 0 {
				// if no one is response for it before, we can start serving immediately
				kv.shard[shardID].State = VALID
			} else if kv.preConfig.Shards[shardID] != kv.gid {
				// if it doesn't belong to me previously, then we start to request the data
				kv.shard[shardID].State = REQUESTDATA
			}
		}
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
	kv.updateShardStateL()
	DPrintf("[%d-%d] commit new config %v", kv.gid, kv.me, kv.curConfig)
	// log.Printf("[%d-%d] commit new config %v", kv.gid, kv.me, kv.curConfig)
	// update commit Index
	kv.commitIndex = msg.CommandIndex
}

func (kv *ShardKV) applyShardData(msg *raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	C := msg.Command.(Command)
	dataLog := C.Data.(ShardDataLog)

	if kv.curConfig.Num != dataLog.ConfigNum {
		return
	}

	// means we have already applied the data, we cann't (but not need not) overwrite it.
	// because there maybe some incoming request which may changed the data
	if kv.shard[dataLog.ShardID].State != REQUESTDATA {
		return
	}

	// with this state change, we can guarantee the shard data will be exactly applied once
	kv.shard[dataLog.ShardID].DB = DuplicateDB(dataLog.DB)
	kv.shard[dataLog.ShardID].ClientSeq = DuplicateClientSeq(dataLog.ClientSeq)
	kv.shard[dataLog.ShardID].State = NOTIFYDELETE
	DPrintf("[%d-%d] applied shard data ShardID=%d configNum=%d", kv.gid, kv.me, dataLog.ShardID, dataLog.ConfigNum)
	// update commit Index
	kv.commitIndex = msg.CommandIndex
}

func (kv *ShardKV) applyDeleteData(msg *raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	C := msg.Command.(Command)
	deleteLog := C.Data.(DeleteDataLog)

	if kv.curConfig.Num != deleteLog.ConfigNum {
		return
	}

	if kv.shard[deleteLog.ShardID].State == INVALID {
		return
	}

	kv.shard[deleteLog.ShardID].DB = make(map[string]string)
	kv.shard[deleteLog.ShardID].ClientSeq = make(map[int64]int)
	kv.shard[deleteLog.ShardID].State = INVALID
	DPrintf("[%d-%d] delete shard data ShardID=%d configNum=%d", kv.gid, kv.me, deleteLog.ShardID, kv.curConfig.Num)
	// log.Printf("[%d-%d] delete shard data ShardID=%d configNum=%d", kv.gid, kv.me, deleteLog.ShardID, kv.curConfig.Num)
	// update commit Index
	kv.commitIndex = msg.CommandIndex
}

func (kv *ShardKV) applyConfirmDelete(msg *raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	C := msg.Command.(Command)
	confirmLog := C.Data.(ConfirmLog)

	if kv.curConfig.Num != confirmLog.ConfigNum {
		return
	}

	if kv.shard[confirmLog.ShardID].State != NOTIFYDELETE {
		return
	}

	DPrintf("[%d-%d] confirm delete data ShardID=%d configNum=%d", kv.gid, kv.me, confirmLog.ShardID, kv.curConfig.Num)
	// log.Printf("[%d-%d] confirm delete data ShardID=%d configNum=%d", kv.gid, kv.me, confirmLog.ShardID, kv.curConfig.Num)
	kv.shard[confirmLog.ShardID].State = VALID
	// update commit Index
	kv.commitIndex = msg.CommandIndex
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
			case SHARDDATA:
				kv.applyShardData(&msg)
			case DELETEDATA:
				kv.applyDeleteData(&msg)
			case EMPTY:
				// do nothing
			case CONFIRMDELETE:
				kv.applyConfirmDelete(&msg)
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

	// before anything, check whether we are the leader
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	
	kv.mu.Lock()

	command := Op {
		Type: GET,
		Key: args.Key,
		KvID: kv.me,
		ClientID: args.ClientID,
		SeqID: args.SeqID,
		ConfigNum : kv.curConfig.Num,
	}

	if kv.checkShardKeyL(args.Key) == false {
		reply.Err = ErrWrongGroup
		DPrintf("[%d-%d] return due to wrong shard %v %v", kv.gid, kv.me, kv.curConfig, kv.shard)
		kv.mu.Unlock()
		return
	}

	shard := key2shard(args.Key)
	if kv.shard[shard].State == REQUESTDATA {
		// although i'm responseable for this data, but i need some time to request the data, so please wait for a second
		reply.Err = ErrTryAgain
		DPrintf("[%d-%d] return due to waiting data", kv.gid, kv.me)
		kv.mu.Unlock()
		return
	}

	if kv.shard[shard].ClientSeq[args.ClientID] >= args.SeqID {
		reply.Err = OK
		reply.Value = kv.shard[shard].DB[args.Key]
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
		if op.Err != OK {
			reply.Err = op.Err
		} else if op.ClientID != args.ClientID || op.SeqID != args.SeqID {
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

	// before anything, check whether we are the leader
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()

	command := Op {
		Type: args.Op,
		Key: args.Key,
		Value: args.Value,
		KvID: kv.me,
		ClientID: args.ClientID,
		SeqID: args.SeqID,
		ConfigNum : kv.curConfig.Num,
	}

	if kv.checkShardKeyL(args.Key) == false {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	shard := key2shard(args.Key)

	if kv.shard[shard].State == REQUESTDATA {
		reply.Err = ErrTryAgain
		kv.mu.Unlock()
		return
	}

	if kv.shard[shard].ClientSeq[args.ClientID] >= args.SeqID {
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
		if op.Err != OK {
			reply.Err = op.Err
		} else if op.ClientID != args.ClientID || op.SeqID != args.SeqID {
			// sanity check here
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
			kv.mu.RLock()
			// DPrintf("[%d-%d] startSnapshot index=%d", kv.gid, kv.me, kv.commitIndex)

			// if we can do COW would be greater
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.shard)
			e.Encode(kv.commitIndex)
			e.Encode(kv.curConfig)
			e.Encode(kv.preConfig)
			commitIndex := kv.commitIndex

			kv.mu.RUnlock()

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
	var preConfig shardctrler.Config
	var curConfig shardctrler.Config

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	if d.Decode(&shard) != nil ||
	   d.Decode(&commitIndex) != nil ||
	   d.Decode(&curConfig) != nil ||
	   d.Decode(&preConfig) != nil {
		DPrintf("[%d-%d] failed to read from persist", kv.gid, kv.me)
	} else {
		kv.shard = shard
		kv.commitIndex = commitIndex
		kv.curConfig = curConfig
		kv.preConfig = preConfig
		DPrintf("[%d-%d] recover from persist, commitIndex=%d", kv.gid, kv.me, kv.commitIndex)
	}
}

func (kv *ShardKV) applySnapshot(snapshot []byte, snapshotTerm, snapshotIndex int) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	var shard [shardctrler.NShards]Shard
	var commitIndex int
	var preConfig shardctrler.Config
	var curConfig shardctrler.Config

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	if d.Decode(&shard) != nil ||
	   d.Decode(&commitIndex) != nil ||
	   d.Decode(&curConfig) != nil ||
	   d.Decode(&preConfig) != nil {
		DPrintf("[%d-%d] failed to read from snapshot", kv.gid, kv.me)
	} else {
		if kv.rf.CondInstallSnapshot(snapshotTerm, snapshotIndex, snapshot) {
			kv.mu.Lock()
			kv.shard = shard
			kv.commitIndex = commitIndex
			kv.curConfig = curConfig
			kv.preConfig = preConfig
			DPrintf("[%d-%d] apply snapshot term=%d index=%d commitIndex=%d", kv.gid, kv.me, snapshotTerm, snapshotIndex, kv.commitIndex)
			kv.mu.Unlock()
		} else {
			DPrintf("[%d-%d] failed to apply snapshot term=%d index=%d", kv.gid, kv.me, snapshotTerm, snapshotIndex)
		}
	}
}

func (kv *ShardKV) debugger() {
	// periodically log the informationn
	for kv.killed() == false {
		time.Sleep(1 * time.Second)
		_, isLeader := kv.rf.GetState()
		term := kv.rf.CheckLogTerm()
		if !isLeader {
			continue
		}

		index := kv.rf.GetLastLogIndex()
		kv.mu.RLock()
		log.Printf("[%d-%d] config %v shardState %v lastLogIndex %d curIndex %d logterm %v", kv.gid, kv.me, kv.curConfig, kv.shard, index, kv.commitIndex, term)
		kv.mu.RUnlock()
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
	labgob.Register(ShardDataLog{})
	labgob.Register(DeleteDataLog{})
	labgob.Register(EmptyLog{})
	labgob.Register(ConfirmLog{})

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
		kv.shard[i].DB = make(map[string]string)
		kv.shard[i].ClientSeq = make(map[int64]int)
		kv.shard[i].State = INVALID
	}

	kv.readPersist()

	go kv.executor()
	go kv.snapshotThread()
	go kv.configPuller()
	go kv.dataPuller()
	go kv.dataDeleter()
	go kv.logChecker()
	// go kv.debugger()

	return kv
}
