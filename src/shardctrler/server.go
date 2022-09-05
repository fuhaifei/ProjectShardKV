package shardctrler

import (
	"bytes"
	"log"
	"sort"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

// func init() {
// 	log.SetOutput(ioutil.Discard)
// }

type ShardCtrler struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	opResMap      map[int64]LastOpResult
	notifyChanMap map[int64]chan int
}

const (
	OpJoin  = "join"
	OpLeave = "leave"
	OpMove  = "move"
	OpQuery = "query"

	MaxWaitTime = 500
)

type Op struct {
	// Your data here.
	ClientStamp int64
	OpStamp     int
	OpType      string //操作结果
	OpParameter []byte //经过编码的输入参数
}

type LastOpResult struct {
	OpStamp int    //最后一个操作的序列号
	Err     Err    //错误结果
	Config  Config //返回Config结果
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	//1.构建操作体
	encoderBuffer := new(bytes.Buffer)
	objectEncoder := labgob.NewEncoder(encoderBuffer)
	objectEncoder.Encode(args.Servers)
	joinOp := Op{ClientStamp: args.ClientStamp, OpStamp: args.OpStamp, OpType: OpJoin, OpParameter: encoderBuffer.Bytes()}
	//2.发起请求
	var opResult LastOpResult
	reply.WrongLeader, opResult = sc.processOperation(joinOp)
	reply.Err = opResult.Err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	//1.构建操作体
	encoderBuffer := new(bytes.Buffer)
	objectEncoder := labgob.NewEncoder(encoderBuffer)
	objectEncoder.Encode(args.GIDs)
	leaveOp := Op{ClientStamp: args.ClientStamp, OpStamp: args.OpStamp, OpType: OpLeave, OpParameter: encoderBuffer.Bytes()}
	//2.发起请求
	var opResult LastOpResult
	reply.WrongLeader, opResult = sc.processOperation(leaveOp)
	reply.Err = opResult.Err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	//1.构建操作体
	encoderBuffer := new(bytes.Buffer)
	objectEncoder := labgob.NewEncoder(encoderBuffer)
	objectEncoder.Encode(args.Shard)
	objectEncoder.Encode(args.GID)
	moveOp := Op{ClientStamp: args.ClientStamp, OpStamp: args.OpStamp, OpType: OpMove, OpParameter: encoderBuffer.Bytes()}
	//2.发起请求
	var opResult LastOpResult
	reply.WrongLeader, opResult = sc.processOperation(moveOp)
	reply.Err = opResult.Err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.

	//1.构建操作体
	encoderBuffer := new(bytes.Buffer)
	objectEncoder := labgob.NewEncoder(encoderBuffer)
	objectEncoder.Encode(args.Num)
	queryOp := Op{ClientStamp: args.ClientStamp, OpStamp: args.OpStamp, OpType: OpQuery, OpParameter: encoderBuffer.Bytes()}
	//2.发起请求
	var opResult LastOpResult
	reply.WrongLeader, opResult = sc.processOperation(queryOp)
	reply.Config = opResult.Config
	reply.Err = opResult.Err
}

func (sc *ShardCtrler) processOperation(op Op) (bool, LastOpResult) {
	opResult := LastOpResult{}
	wrongLeader := false

	//1.判断请求是否已经处理过
	sc.mu.RLock()
	cachedRes, ok := sc.opResMap[op.ClientStamp]
	if ok && cachedRes.OpStamp == op.OpStamp {
		//需要判断OpStamp，因为访问完不能删除，有可能新请求读到老op
		opResult = cachedRes
		sc.mu.RUnlock()
		//log.Printf("Shardctrler-server %v return reqeust %v , result:%v(already excuted)", sc.me, op.OpType, opResult)
		return wrongLeader, opResult
	}
	sc.mu.RUnlock()
	//2.提交操作日志
	sc.mu.Lock()
	_, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		sc.mu.Unlock()
		wrongLeader = true
		opResult.Err = ErrWrongLeader
		//log.Printf("Shardctrler-server %v return %v reqeust %v:%v, (not leader)", sc.me, op.OpType, op.ClientStamp, op.OpStamp)
		return wrongLeader, opResult
	}
	//3.等待响应结果
	notifyChan := make(chan int, 1)
	sc.notifyChanMap[op.ClientStamp] = notifyChan
	sc.mu.Unlock()
	timeout := time.Now().UnixMilli() + MaxWaitTime
	for time.Now().UnixMilli() < timeout {
		select {
		case <-notifyChan:
			sc.mu.RLock()
			cachedRes := sc.opResMap[op.ClientStamp]
			sc.mu.RUnlock()
			if cachedRes.OpStamp == op.OpStamp {
				opResult = cachedRes
				//log.Printf("Shardctrler-server %v return reqeust %v %v:%v result:%v", sc.me, op.OpType, op.ClientStamp, op.OpStamp, opResult)
				return wrongLeader, opResult
			}
		default:
			//休眠10微妙
			time.Sleep(1 * time.Millisecond)
		}
	}
	opResult.Err = ErrTimeOut
	//认为超时后，删除对应的chan，表示不再接收返回结果
	sc.mu.Lock()
	sc.notifyChanMap[op.ClientStamp] = nil
	sc.mu.Unlock()
	//log.Printf("Shardctrler-server %v return reqeust %v %v:%v {timeout}", sc.me, op.OpType, op.ClientStamp, op.OpStamp)
	return wrongLeader, opResult
}

func (sc *ShardCtrler) reallocate(newConfig Config) Config {
	allGroupId := make([]int, 0)
	groupShardNumMap := make(map[int]int)
	reallocateShards := make([]int, 0)
	//首先统计当前gruop数量
	for key, _ := range newConfig.Groups {
		groupShardNumMap[key] = 0
		allGroupId = append(allGroupId, key)
	}
	if len(allGroupId) == 0 {
		//重置为初始化配置
		newConfig = Config{}
		newConfig.Groups = map[int][]string{}
		return newConfig
	}
	//将shard超过average的日志重新分配
	averageShard := len(newConfig.Shards) / len(allGroupId)
	remindShard := len(newConfig.Shards) % len(allGroupId)
	for shard, group := range newConfig.Shards {
		_, ok := newConfig.Groups[group]
		if !ok {
			//shard所属gruop被删除的情况
			reallocateShards = append(reallocateShards, shard)
		} else {
			numShard := groupShardNumMap[group]
			//如何判断一个group是否超量（关键）：管理shard数量 > averageShard 或者 管理shard数量 == averageShard 且此时可以管理averageShard + 1个shard的机会已经用尽
			if numShard > averageShard || (numShard == averageShard && remindShard == 0) {
				reallocateShards = append(reallocateShards, shard)
			} else {
				//如果当前group管理shard数量为averageShard，再分配一个shard，当前group管理了averageShard + 1个shard，则需要占用一个管理averageShard + 1的名额
				if numShard == averageShard {
					remindShard--
				}
				groupShardNumMap[group] += 1
			}
		}
	}
	//开始重新分配
	//排序groupId（转化为确定性操作）
	sort.Ints(allGroupId)
	startIndex := 0
	for _, gruopId := range allGroupId {
		shardNum := groupShardNumMap[gruopId]
		if averageShard-shardNum > 0 {
			for i := startIndex; i < startIndex+(averageShard-shardNum); i++ {
				newConfig.Shards[reallocateShards[i]] = gruopId
			}
			startIndex += averageShard - shardNum
			shardNum = averageShard
		}
		//二次判断，是否添加到平均值加1
		if remindShard > 0 && shardNum == averageShard {
			newConfig.Shards[reallocateShards[startIndex]] = gruopId
			startIndex++
			remindShard--
		}
		if startIndex == len(reallocateShards) {
			break
		}
	}
	return newConfig
}

func (sc *ShardCtrler) applyCommitLog() {
	for m := range sc.applyCh {
		if m.CommandValid {
			//应用信息
			opCommand := m.Command.(Op)
			cachedRes, ok := sc.opResMap[opCommand.ClientStamp]
			if !ok || cachedRes.OpStamp != opCommand.OpStamp {
				//log.Printf("----------------------%v", opCommand)
				r := bytes.NewBuffer(opCommand.OpParameter)
				paramDecoder := labgob.NewDecoder(r)
				opRes := LastOpResult{}
				sc.mu.Lock()
				if opCommand.OpType == OpQuery {
					//查找结果
					var queryNum int
					if paramDecoder.Decode(&queryNum) != nil {
						//log.Panicf("Shardctrler-server:%v process qeury decode faild %v {clientStamp:%v, opStamp:%v}", sc.me, paramDecoder.Decode(&queryNum), opCommand.ClientStamp, opCommand.OpStamp)
					}
					if queryNum == -1 || queryNum >= len(sc.configs) {
						opRes.Config = sc.configs[len(sc.configs)-1]
					} else {
						opRes.Config = sc.configs[queryNum]
					}
					//log.Printf("Shardctrler-server:%v process qeury %v success {clientStamp:%v, opStamp:%v}", sc.me, queryNum, opCommand.ClientStamp, opCommand.OpStamp)
				} else {
					newConig := Config{}
					//配置编号加一
					newConig.Num = sc.configs[len(sc.configs)-1].Num + 1
					//复制Shard数组
					for i := range newConig.Shards {
						newConig.Shards[i] = sc.configs[len(sc.configs)-1].Shards[i]
					}
					//复制map
					newConig.Groups = make(map[int][]string)
					for key, value := range sc.configs[len(sc.configs)-1].Groups {
						newConig.Groups[key] = value
					}
					if opCommand.OpType == OpMove {
						var moveShard int
						var toGroup int
						if paramDecoder.Decode(&moveShard) != nil || paramDecoder.Decode(&toGroup) != nil {
							log.Panicf("server:%v process move decode faild {clientStamp:%v, opStamp:%v}", sc.me, opCommand.ClientStamp, opCommand.OpStamp)
						}
						_, ok := newConig.Groups[toGroup]
						if !ok {
							//log.Printf("server:%v process move group not exist {clientStamp:%v, opStamp:%v}", sc.me, opCommand.ClientStamp, opCommand.OpStamp)
						} else {
							newConig.Shards[moveShard] = toGroup
							//log.Printf("server:%v process move group sucess {clientStamp:%v, opStamp:%v}", sc.me, opCommand.ClientStamp, opCommand.OpStamp)
						}
					} else if opCommand.OpType == OpLeave {
						var leaveIds []int
						if paramDecoder.Decode(&leaveIds) != nil {
							log.Panicf("server:%v process leave decode faild {clientStamp:%v, opStamp:%v}", sc.me, opCommand.ClientStamp, opCommand.OpStamp)
						}
						//遍历判断是否所有leaderId都有效(有可能当前gruop已经删除)
						for _, id := range leaveIds {
							_, ok := newConig.Groups[id]
							if !ok {
								log.Panicf("server:%v process leave gruop not exist {clientStamp:%v, opStamp:%v}", sc.me, opCommand.ClientStamp, opCommand.OpStamp)
							}
							delete(newConig.Groups, id)
						}
						//进行重新分配
						newConig = sc.reallocate(newConig)
						log.Printf("server:%v process move group sucess {clientStamp:%v, opStamp:%v}", sc.me, opCommand.ClientStamp, opCommand.OpStamp)
					} else {
						var newServers map[int][]string
						if paramDecoder.Decode(&newServers) != nil {
							log.Panicf("server:%v process join decode faild {clientStamp:%v, opStamp:%v}", sc.me, opCommand.ClientStamp, opCommand.OpStamp)
						}
						//遍历判断是否相同id的group
						for key, value := range newServers {
							_, ok := newConig.Groups[key]
							if ok {
								log.Panicf("server:%v process join fasiled {clientStamp:%v, opStamp:%v} already exits", sc.me, opCommand.ClientStamp, opCommand.OpStamp)
							}
							newConig.Groups[key] = value
						}
						//进行重新分配
						newConig = sc.reallocate(newConig)
						//log.Printf("server:%v process join group sucess {clientStamp:%v, opStamp:%v}", sc.me, opCommand.ClientStamp, opCommand.OpStamp)
					}
					//添加新配置
					sc.configs = append(sc.configs, newConig)
				}
				//结果记录在map中
				sc.opResMap[opCommand.ClientStamp] = opRes
				//判断是否需要通知
				notifyChan := sc.notifyChanMap[opCommand.ClientStamp]
				if notifyChan != nil {
					//首先删除
					sc.notifyChanMap[opCommand.ClientStamp] = nil
					notifyChan <- 1
				}
				sc.mu.Unlock()
			}
		} else if m.SnapshotValid {
			//log.Printf("Shardctrler-server:%v received snapshot request", sc.me)
		}
	}
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.opResMap = map[int64]LastOpResult{}
	sc.notifyChanMap = map[int64]chan int{}

	//应用提交log
	go sc.applyCommitLog()

	return sc
}
