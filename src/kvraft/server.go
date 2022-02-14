package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
	"bytes"
)

const Debug = false

const CommonInterval = 10

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	PUT		int = 0
	APPEND	int = 1
	GET		int = 2
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type	int
	Value	string
	Key		string

	// who is response to respond this request
	KvID 		int
	ClientID	int64
	// requestID
	SeqID		int
}

type Entry struct {
	putAppendReply		*PutAppendReply
	getReply			*GetReply
	term 				int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// kv storage
	db					map[string]string
	// index -> chan
	commitChannel		map[int]chan Op
	// clientID -> seqID
	clientSeq			map[int64]int
	// commit index used in snapshot
	commitIndex 		int

	// timeMap 			map[int]time.Time
}

func (kv *KVServer) executor() {
	for kv.killed() == false {
		msg := <- kv.applyCh


		if msg.CommandValid {
			kv.mu.Lock()
			command := msg.Command.(Op)
			ch, exists := kv.commitChannel[msg.CommandIndex]
			
			switch command.Type {
			case GET:
				value, appear := kv.db[command.Key]
				// check whether i'm responsable to send the command back to channel
				if command.KvID == kv.me && exists {
					if !appear {
						command.Value = ""
					} else {
						command.Value = value
					}
				}
			case APPEND:
				value, appear := kv.db[command.Key]
				// first check whether we have applied this command
				if kv.clientSeq[command.ClientID] < command.SeqID {
					if !appear {
						kv.db[command.Key] = command.Value
					} else {
						kv.db[command.Key] = value + command.Value
					}
				}
				
			case PUT:
				if kv.clientSeq[command.ClientID] < command.SeqID {
					kv.db[command.Key] = command.Value
				}
			}
			// update commit Index
			kv.commitIndex = msg.CommandIndex

			// update the seqID to prevent duplicated operation
			if kv.clientSeq[command.ClientID] < command.SeqID {
				kv.clientSeq[command.ClientID] = command.SeqID
			}

			kv.mu.Unlock()

			// DPrintf("[%d] index %d KvID %d exists %v", kv.me, msg.CommandIndex, command.KvID, exists)
			if command.KvID == kv.me && exists {
				DPrintf("[%d] Send Op though apply channel index=%d ClientID=%d SeqID=%d", kv.me, msg.CommandIndex, command.ClientID, command.SeqID)
				// DPrintf("[%d] commit time used %v", kv.me, time.Since(kv.timeMap[msg.CommandIndex]))
				ch <- command
			}
		} else if msg.SnapshotValid {
			go kv.applySnapshot(msg.Snapshot, msg.SnapshotTerm, msg.SnapshotIndex)
		}
	}
}

func (kv *KVServer) applySnapshot(snapshot []byte, snapshotTerm, snapshotIndex int) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	var db map[string]string
	var clientSeq map[int64]int
	var commitIndex int

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	if d.Decode(&db) != nil ||
	   d.Decode(&clientSeq) != nil ||
	   d.Decode(&commitIndex) != nil {
		DPrintf("[%d] failed to read from snapshot", kv.me)
	} else {
		if kv.rf.CondInstallSnapshot(snapshotTerm, snapshotIndex, snapshot) {
			kv.mu.Lock()
			kv.clientSeq = clientSeq
			kv.db = db
			kv.commitIndex = commitIndex
			DPrintf("[%d] apply snapshot term=%d index=%d commitIndex=%d", kv.me, snapshotTerm, snapshotIndex, kv.commitIndex)
			kv.mu.Unlock()
		} else {
			DPrintf("[%d] failed to apply snapshot term=%d index=%d", kv.me, snapshotTerm, snapshotIndex)
		}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	command := Op{
		Type: GET,
		Key: args.Key,
		KvID: kv.me,
		ClientID: args.ClientID,
		SeqID: args.SeqID,
	}

	kv.mu.Lock()
	index, term, isLeader := kv.rf.Start(command)
	// kv.timeMap[index] = time.Now()

	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	DPrintf("[%d] start Get command, ClientID=%d SeqID=%d Key=%v", kv.me, args.ClientID, args.SeqID, args.Key)

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
		DPrintf("[%d] received reply index=%d ClientID=%d SeqID=%d", kv.me, index, op.ClientID, op.SeqID)
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

// the reason i use GetChannel to construct the channel instead of in the RequestRPC
// is the commitThread may get ahead of us, so we can't guarantee who is the first one
// to reach this channel
// PLEASE HOLD THE LOCK WHEN CALLING THIS METHOD
func (kv *KVServer) GetChannelL(index int) chan Op {
	ch, exists := kv.commitChannel[index]
	if !exists {
		ch = make(chan Op, 1)
		kv.commitChannel[index] = ch
	}
	return ch
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	command := Op{
		Type: args.Type,
		Key: args.Key,
		Value: args.Value,
		KvID: kv.me,
		ClientID: args.ClientID,
		SeqID: args.SeqID,
	}

	kv.mu.Lock()
	index, term, isLeader := kv.rf.Start(command)
	// kv.timeMap[index] = time.Now()

	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("[%d] start PutAppend command, ClientID=%d SeqID=%d Key=%v Value=%v", kv.me, args.ClientID, args.SeqID, args.Key, args.Value)

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
		DPrintf("[%d] received reply index=%d ClientID=%d SeqID=%d", kv.me, index, op.ClientID, op.SeqID)
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

func (kv *KVServer) snapshotThread() {
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
			DPrintf("[%d] startSnapshot index=%d", kv.me, kv.commitIndex)

			// if we can do COW would be greater
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.db)
			e.Encode(kv.clientSeq)
			e.Encode(kv.commitIndex)
			commitIndex := kv.commitIndex

			kv.mu.Unlock()

			data := w.Bytes()
			kv.rf.Snapshot(commitIndex, data)
		}

	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) readPersist() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	snapshot := kv.rf.ReadSnapshot()
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	
	var db map[string]string
	var clientSeq map[int64]int
	var commitIndex int

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if d.Decode(&db) != nil ||
	   d.Decode(&clientSeq) != nil ||
	   d.Decode(&commitIndex) != nil {
		DPrintf("[%d] failed to read from persist", kv.me)
	} else {
		kv.clientSeq = clientSeq
		kv.db = db
		kv.commitIndex = commitIndex
		DPrintf("[%d] recover from persist, commitIndex=%d", kv.me, kv.commitIndex)
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.db = make(map[string]string)
	kv.commitChannel = make(map[int]chan Op)
	kv.clientSeq = make(map[int64]int)
	// kv.timeMap = make(map[int]time.Time)
	kv.commitIndex = 0

	kv.readPersist()

	go kv.executor()
	go kv.snapshotThread()

	return kv
}
