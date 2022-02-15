package shardctrler

//
// Shardctrler clerk.
//

import "6.824/labrpc"
import "crypto/rand"
import "math/big"

const TimeoutThreshold = 3

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	lastLeader	int
	n 			int
	ID			int64
	seqID 		int
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
	ck.lastLeader = 0
	ck.n = len(servers)
	ck.ID = nrand()
	ck.seqID = 0

	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := QueryArgs{}

	args.Num = num
	ck.seqID++
	seqID := ck.seqID
	timeoutCnt := 0

	args.ClientID = ck.ID
	args.SeqID = seqID

	for true {
		reply := QueryReply{}
		ok := ck.servers[ck.lastLeader].Call("ShardCtrler.Query", &args, &reply)

		if !ok || reply.WrongLeader {
			ck.lastLeader = (ck.lastLeader + 1) % ck.n
			continue
		}

		switch reply.Err {
		case OK:
			return reply.Config
		case ErrTimeout:
			timeoutCnt++
			if timeoutCnt > TimeoutThreshold {
				timeoutCnt = 0
				ck.lastLeader = (ck.lastLeader + 1) % ck.n
			}
		}
	}

	return Config{}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := JoinArgs{}
	args.Servers = servers
	ck.seqID++
	seqID := ck.seqID
	timeoutCnt := 0

	args.ClientID = ck.ID
	args.SeqID = seqID

	DPrintf("[%d] call Join %v", ck.ID, args)
	for true {
		reply := JoinReply{}
		ok := ck.servers[ck.lastLeader].Call("ShardCtrler.Join", &args, &reply)

		if !ok || reply.WrongLeader {
			ck.lastLeader = (ck.lastLeader + 1) % ck.n
			continue
		}

		switch reply.Err {
		case OK:
			return
		case ErrTimeout:
			timeoutCnt++
			if timeoutCnt > TimeoutThreshold {
				timeoutCnt = 0
				ck.lastLeader = (ck.lastLeader + 1) % ck.n
			}
		}
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := LeaveArgs{}
	args.GIDs = gids
	ck.seqID++
	seqID := ck.seqID
	timeoutCnt := 0

	args.ClientID = ck.ID
	args.SeqID = seqID

	DPrintf("[%d] call leave %v", ck.ID, args)
	for true {
		reply := LeaveReply{}
		ok := ck.servers[ck.lastLeader].Call("ShardCtrler.Leave", &args, &reply)

		if !ok || reply.WrongLeader {
			ck.lastLeader = (ck.lastLeader + 1) % ck.n
			continue
		}

		switch reply.Err {
		case OK:
			return
		case ErrTimeout:
			timeoutCnt++
			if timeoutCnt > TimeoutThreshold {
				timeoutCnt = 0
				ck.lastLeader = (ck.lastLeader + 1) % ck.n
			}
		}
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := MoveArgs{}
	args.Shard = shard
	args.GID = gid
	ck.seqID++
	seqID := ck.seqID
	timeoutCnt := 0

	args.ClientID = ck.ID
	args.SeqID = seqID

	DPrintf("[%d] call move %v", ck.ID, args)
	for true {
		reply := MoveReply{}
		ok := ck.servers[ck.lastLeader].Call("ShardCtrler.Move", &args, &reply)

		if !ok || reply.WrongLeader {
			ck.lastLeader = (ck.lastLeader + 1) % ck.n
			continue
		}

		switch reply.Err {
		case OK:
			return
		case ErrTimeout:
			timeoutCnt++
			if timeoutCnt > TimeoutThreshold {
				timeoutCnt = 0
				ck.lastLeader = (ck.lastLeader + 1) % ck.n
			}
		}
	}
}
