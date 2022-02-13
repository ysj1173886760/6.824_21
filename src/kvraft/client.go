package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader	int
	n 			int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.lastLeader = 0
	ck.n = len(servers)

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	args := GetArgs{}

	args.Key = key

	for true {
		reply := GetReply{}
		ok := ck.servers[ck.lastLeader].Call("KVServer.Get", &args, &reply)

		if !ok {
			ck.lastLeader = (ck.lastLeader + 1) % ck.n
			continue
		}

		switch reply.Err {
		case OK: 
			return reply.Value
		case ErrNoKey:
			return ""
		case ErrWrongLeader:
			ck.lastLeader = (ck.lastLeader + 1) % ck.n
			continue
		}
	}
	
	// unreachable
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}

	switch op {
	case "Put":
		args.Type = PUT
	case "Append":
		args.Type = APPEND
	}
	args.Key = key
	args.Value = value

	for true {
		reply := PutAppendReply{}
		ok := ck.servers[ck.lastLeader].Call("KVServer.PutAppend", &args, &reply)

		if !ok {
			ck.lastLeader = (ck.lastLeader + 1) % ck.n
			continue
		}

		switch reply.Err {
		case OK: 
			return
		case ErrNoKey:
			return
		case ErrWrongLeader:
			ck.lastLeader = (ck.lastLeader + 1) % ck.n
			continue
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
