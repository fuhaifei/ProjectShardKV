package kvraft

import (
	"crypto/rand"
	"log"
	"math/big"

	"6.824/labrpc"
)

const (
	WaitReturnTime = 500
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clentStamp   int64 //标记客户端序列号
	opStamp      int64 //标记当前操作序列号
	leaderServer int   //记录当前leader
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
	ck.clentStamp = nrand() //随机生成当前client编号
	ck.opStamp = 0          //操作编号初始化为0
	bigx, _ := rand.Int(rand.Reader, big.NewInt(int64(len(servers))))
	ck.leaderServer = int(bigx.Int64()) //随机初始化当前leader，server

	log.Printf("client:%v seart with init server:%v", ck.clentStamp, ck.leaderServer)
	return ck
}

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

	// You will have to modify this function.(不考虑并发情况)
	result_value := ""
	args := GetArgs{ClientStamp: ck.clentStamp, OpStamp: ck.opStamp, Key: key}
	for i := ck.leaderServer; ; i = (i + 1) % len(ck.servers) {
		reply := GetReply{}
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if ok {
			if reply.Err == ErrTimeOut {
				log.Printf("client:%v op:%v get%v request timeout ", ck.clentStamp, ck.opStamp, key)
			} else if reply.Err == ErrWrongLeader {
				log.Printf("client:%v get%v wrongleader", ck.clentStamp, ck.opStamp)
			} else {
				if reply.Err == ErrNoKey {
					log.Printf("client:%v op:%v get key：%v doesn't exist ", ck.clentStamp, ck.opStamp, key)
					break
				}
				log.Printf("client:%v op:%v get key：%v value:%v success", ck.clentStamp, ck.opStamp, key, reply.Value)
				//修改leaderServer
				ck.leaderServer = i
				result_value = reply.Value
				break
			}
		} else {
			log.Printf("client:%v op:%v get key：%v rpc failed", ck.clentStamp, ck.opStamp, key)
		}
	}
	//最后操作符加一
	ck.opStamp++
	return result_value
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
	// You will have to modify this function.
	args := PutAppendArgs{ClientStamp: ck.clentStamp, OpStamp: ck.opStamp, Key: key, Value: value, Op: op}
	for i := ck.leaderServer; ; i = (i + 1) % len(ck.servers) {
		reply := PutAppendReply{}
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			if reply.Err == ErrTimeOut {
				log.Printf("client:%v op(%v) %v key:%v value:%v request timeout ", ck.clentStamp, op, ck.opStamp, key, value)
			} else if reply.Err == ErrWrongLeader {
				log.Printf("client:%v op(%v) %v key:%v value:%v wrong leader", ck.clentStamp, op, ck.opStamp, key, value)
			} else {
				if reply.Err == ErrNoKey {
					log.Printf("client:%v op(%v) %v key:%v value:%v doesn't exist ", ck.clentStamp, op, ck.opStamp, key, value)
					break
				}
				log.Printf("client:%v op(%v) %v key:%v value:%v success", ck.clentStamp, op, ck.opStamp, key, value)
				//修改leaderServer
				ck.leaderServer = i
				break
			}
		} else {
			log.Printf("client:%v op(%v) %v key:%v value:%v rpc failed", ck.clentStamp, op, ck.opStamp, key, value)
		}
	}
	//最后操作符加一
	ck.opStamp++
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OpPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpAppend)
}
