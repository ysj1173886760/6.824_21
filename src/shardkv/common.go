package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout	   = "ErrTimeout"
	ErrTryAgain	   = "ErrTryAgain"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	ClientID int64
	SeqID int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	ClientID int64
	SeqID int
}

type GetReply struct {
	Err   Err
	Value string
}

type RequestDataArgs struct {
	ConfigNum	int
	ShardID 	int
	
	// for debugging
	GID 		int
}

type RequestDataReply struct {
	Valid 		bool
	DB 			map[string]string
	ClientSeq 	map[int64]int

	// for debugging
	GID 		int
}

type RequestDeleteArgs struct {
	ShardID 	int
	ConfigNum 	int

	GID 		int
}

type RequestDeleteReply struct {
	Deleted 	bool
	WrongLeader bool

	GID 		int
}