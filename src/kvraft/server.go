package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

const BufferSize = 1000

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
	ID 		int
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
	db					map[string]string
	currentCommitIndex	int

	cicularBuffer		[BufferSize]Entry
}

func (kv *KVServer) executor() {
	for kv.killed() == false {
		msg := <- kv.applyCh

		kv.mu.Lock()
		command := msg.Command.(Op)
		if msg.CommandValid {
			switch command.Type {
			case GET:
				reply := kv.cicularBuffer[msg.CommandIndex % BufferSize].getReply
				value, appear := kv.db[command.Key]
				if command.ID == kv.me {
					if !appear {
						reply.Err = ErrNoKey
					} else {
						reply.Err = OK
						reply.Value = value
					}
				}
			case APPEND:
				reply := kv.cicularBuffer[msg.CommandIndex % BufferSize].putAppendReply
				value, appear := kv.db[command.Key]
				if !appear {
					kv.db[command.Key] = command.Value
				} else {
					kv.db[command.Key] = value + command.Value
				}
				if command.ID == kv.me {
					reply.Err = OK
				}
			case PUT:
				reply := kv.cicularBuffer[msg.CommandIndex % BufferSize].putAppendReply
				kv.db[command.Key] = command.Value
				if command.ID == kv.me {
					reply.Err = OK
				}
			}
		}
		kv.currentCommitIndex = msg.CommandIndex
		kv.mu.Unlock()
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	command := Op{ Type: GET, Key: args.Key, ID: kv.me}
	index, term, isLeader := kv.rf.Start(command)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// put the reply variable into buffer
	kv.mu.Lock()
	DPrintf("[%d] fill cicularBuffer, index=%d", kv.me, index % BufferSize)
	kv.cicularBuffer[index % BufferSize].getReply = reply
	kv.mu.Unlock()

	for kv.killed() == false {
		time.Sleep(time.Millisecond * 10)
		kv.mu.Lock()
		currentCommitIndex := kv.currentCommitIndex
		kv.mu.Unlock()
		if currentCommitIndex >= index {
			break;
		}
	}

	// check Am I still the leader
	new_term, new_leader := kv.rf.GetState()
	if new_term != term || new_leader == false {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	command := Op{ Type: args.Type, Key: args.Key, Value: args.Value, ID: kv.me }
	index, term, isLeader := kv.rf.Start(command)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// put the reply variable into buffer
	kv.mu.Lock()
	DPrintf("[%d] fill cicularBuffer, index=%d", kv.me, index % BufferSize)
	kv.cicularBuffer[index % BufferSize].putAppendReply = reply
	kv.mu.Unlock()

	for kv.killed() == false {
		time.Sleep(time.Millisecond * 10)
		kv.mu.Lock()
		currentCommitIndex := kv.currentCommitIndex
		kv.mu.Unlock()
		if currentCommitIndex >= index {
			break;
		}
	}

	// check Am I still the leader
	new_term, new_leader := kv.rf.GetState()
	if new_term != term || new_leader == false {
		reply.Err = ErrWrongLeader
		return
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

	kv.currentCommitIndex = 0
	kv.db = make(map[string]string)

	go kv.executor()

	return kv
}
