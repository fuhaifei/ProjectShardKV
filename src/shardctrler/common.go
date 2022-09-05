package shardctrler

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK             = "OK"
	ErrWrongLeader = "WrongLeader"
	ErrTimeOut     = "TimeOut"
)

type Err string

type JoinArgs struct {
	Servers     map[int][]string // new GID -> servers mappings
	ClientStamp int64            //client编号
	OpStamp     int              //操作编号
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs        []int
	ClientStamp int64 //client编号
	OpStamp     int   //操作编号
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard       int
	GID         int
	ClientStamp int64 //client编号
	OpStamp     int   //操作编号
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num         int   // desired config number
	ClientStamp int64 //client编号
	OpStamp     int   //操作编号
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
