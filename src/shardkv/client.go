package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
import "6.824/shardctrler"
import "time"

const RetryCount = 3
const RetryInterval = 30

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	
	// for every group of server, we have to remember the lastLeader
	// GID -> lastLeader
	// lastLeader 	map[int]int
	ID 			int64
	SeqID 		int
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	
	ck.ID = nrand()

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key

	ck.SeqID++
	args.ClientID = ck.ID
	args.SeqID = ck.SeqID

	DPrintf("[%d] start Get args=%v", ck.ID, args)
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si, wrongGroup := 0, false; si < len(servers) && !wrongGroup; si++ {
				for cnt := 0; cnt < RetryCount; cnt++ {
					srv := ck.make_end(servers[si])
					var reply GetReply
					// log.Printf("[%d] Get request to %v", ck.ID, servers[si])
					ok := srv.Call("ShardKV.Get", &args, &reply)
					if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
						return reply.Value
					}
					if ok && (reply.Err == ErrWrongGroup) {
						wrongGroup = true
						break
					}
					if ok && reply.Err == ErrWrongLeader {
						// switch the leader
						break
					}
					if ok && (reply.Err == ErrTimeout || reply.Err == ErrTryAgain) {
						// we retry this operation
					}
					// ... not ok
					// retry 
					time.Sleep(time.Millisecond * time.Duration(RetryInterval))
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op

	ck.SeqID++
	args.ClientID = ck.ID
	args.SeqID = ck.SeqID

	DPrintf("[%d] start PutAppend args=%v", ck.ID, args)
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si, wrongGroup := 0, false; si < len(servers) && !wrongGroup; si++ {
				for cnt := 0; cnt < RetryCount; cnt++ {
					srv := ck.make_end(servers[si])
					var reply PutAppendReply
					// log.Printf("[%d] PutAppend request to %v", ck.ID, servers[si])
					ok := srv.Call("ShardKV.PutAppend", &args, &reply)
					if ok && reply.Err == OK {
						return
					}
					if ok && reply.Err == ErrWrongGroup {
						wrongGroup = true
						break
					}
					if ok && reply.Err == ErrWrongLeader {
						// switch the leader
						break
					}
					if ok && (reply.Err == ErrTimeout || reply.Err == ErrTryAgain) {
						// we retry this operation
					}
					// ... not ok
					time.Sleep(time.Millisecond * time.Duration(RetryInterval))
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
