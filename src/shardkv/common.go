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
	ErrTimeOut     = "ErrTimeOut"
	ErrNotReady    = "ErrNotReady"

	//两个Gruop之间交互的错误类型
	ErrOutDate    = "ErrOutDate"
	ErrNotDeleted = "ErrNotDeleted"

	//操作类型
	OpGet    = "Get"
	OpPut    = "Put"
	OpAppend = "Append"

	//切换配置的相关操作
	ConfigChange  = "Change"
	ConfigMoveIn  = "MoveIn"
	ConfigInAck   = "InAck"
	ConfigMoveOut = "MoveOut"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	ClientStamp int64
	OpStamp     int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientStamp int64
	OpStamp     int64
}

type GetReply struct {
	Err   Err
	Value string
}

//
// 不同gruop之间交换gruop的请求内容
//

type EmptyReply struct {
}

type MigrateShardArgs struct {
	ConfigNum int
	ShardId   int
}

type MigrateShardReply struct {
	Err      Err
	AimShard ShardData
}

type MigrateAckArgs struct {
	ConfigNum int
	AckShard  int
}

type MigrateAckReply struct {
	Err Err
}
