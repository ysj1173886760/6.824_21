package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key		string
	Value	string
	Type	int
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID int64
	SeqID int
}

type PutAppendReply struct {
	Err 	Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID int64
	SeqID int
}

type GetReply struct {
	Err   	Err
	Value 	string
}
