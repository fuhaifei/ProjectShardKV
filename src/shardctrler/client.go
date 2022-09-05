package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientStamp int64 //client编号
	opStamp     int   //操作编号，用来去重
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
	// Your code here.
	ck.clientStamp = nrand()
	ck.opStamp = 0
	//log.Printf("Shardctrler-client:%v start", ck.clientStamp)
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.ClientStamp = ck.clientStamp
	args.OpStamp = ck.opStamp

	//log.Printf("Shardctrler-client:%v send query:%v", ck.clientStamp, args)
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				//执行成功，增加client号码
				ck.clientStamp++
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientStamp = ck.clientStamp
	args.OpStamp = ck.opStamp
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				//log.Printf("Shardctrler-client:%v send join:%v", ck.clientStamp, servers)
				ck.clientStamp++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientStamp = ck.clientStamp
	args.OpStamp = ck.opStamp

	//log.Printf("Shardctrler-client:%v send leave:%v", ck.clientStamp, args)
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.clientStamp++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientStamp = ck.clientStamp
	args.OpStamp = ck.opStamp

	//log.Printf("Shardctrler-client:%v send move:%v", ck.clientStamp, args)
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.clientStamp++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
