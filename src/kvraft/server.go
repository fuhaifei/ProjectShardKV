package kvraft

import (
	"bytes"
	"io/ioutil"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

func init() {
	log.SetOutput(ioutil.Discard)
}

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	//数值型常数
	MaxWaitTime = 300
)

//临时存储执行结果
type LastOpResult struct {
	OpStamp int64  //最后一个操作的序列号
	Err     Err    //错误结果
	Value   string //返回值
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientStamp int64
	OpStamp     int64
	Key         string
	Value       string
	OpType      string // "Put"  "Append" "Get"
}

// type OpChan struct {
// 	notifyChan chan int
// 	opStamp    int64
// }

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvStore map[string]string //存储值的Map
	//针对不同client暂存状态
	opResStore      map[int64]LastOpResult      //存储操作结果
	notifyChanStore map[int64]chan LastOpResult //用来通知返回结果的chan

	//用来记录提交日志index的
	lastAppliedIndex int
}

func (kv *KVServer) processOperation(op Op) (Err, string) {
	// Your code here.
	var resultError Err
	resultValue := ""
	//1.首先判断是否存在结果
	kv.mu.RLock()
	opRes, ok := kv.opResStore[op.ClientStamp]
	if ok && opRes.OpStamp == op.OpStamp {
		//需要判断OpStamp，因为访问完不能删除，有可能新请求读到老op
		resultError, resultValue = opRes.Err, opRes.Value
		kv.mu.RUnlock()
		log.Printf("server %v return reqeust %v, err:%v, value:%v(already excuted)", kv.me, op.OpType, resultError, resultValue)
		return resultError, resultValue
	}
	kv.mu.RUnlock()
	//2.发起日志写入请求
	kv.mu.Lock()
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		resultError = ErrWrongLeader
		log.Printf("server %v return reqeust %v, err:%v, value:%v", kv.me, op.OpType, resultError, resultValue)
		return resultError, resultValue
	}
	//添加applyChan
	notifyChan := make(chan LastOpResult, 1)
	kv.notifyChanStore[op.ClientStamp] = notifyChan
	kv.mu.Unlock()
	timeout := time.Now().UnixMilli() + MaxWaitTime
	for time.Now().UnixMilli() < timeout {
		select {
		case opRes := <-notifyChan:
			if opRes.OpStamp == op.OpStamp {
				kv.mu.Lock()
				kv.notifyChanStore[op.ClientStamp] = nil
				kv.mu.Unlock()
				resultError = opRes.Err
				resultValue = opRes.Value
				log.Printf("server %v return reqeust %v, err:%v, value:%v(request:%v, reesult:%v client stamp:%v)", kv.me, op.OpType, resultError, resultValue, op.OpStamp, opRes.OpStamp, op.ClientStamp)
				return resultError, resultValue
			}
		default:
			//休眠10微妙
			time.Sleep(1 * time.Millisecond)
		}
	}
	resultError = ErrTimeOut
	//认为超时后，删除对应的chan，表示不再接收返回结果
	kv.mu.Lock()
	kv.notifyChanStore[op.ClientStamp] = nil
	kv.mu.Unlock()

	log.Printf("server %v return reqeust %v, err:%v, value:%v", kv.me, op.OpType, resultError, resultValue)
	return resultError, resultValue
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//提交空日志
	getOp := Op{ClientStamp: args.ClientStamp, OpStamp: args.OpStamp, OpType: OpGet, Key: args.Key}
	reply.Err, reply.Value = kv.processOperation(getOp)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	putOp := Op{ClientStamp: args.ClientStamp, OpStamp: args.OpStamp, OpType: args.Op, Key: args.Key, Value: args.Value}
	reply.Err, _ = kv.processOperation(putOp)
}

func (kv *KVServer) snapshotStatus() {
	//首先将状态转化为byte数组
	snapshotBuffer := new(bytes.Buffer)
	snapshotEncoder := labgob.NewEncoder(snapshotBuffer)
	kv.mu.RLock()
	//保存kv状态和此时的历史结果（便于去重）
	snapshotEncoder.Encode(kv.kvStore)
	snapshotEncoder.Encode(kv.opResStore)
	snapShotIndex := kv.lastAppliedIndex
	kv.mu.RUnlock()
	//调用快照
	kv.rf.Snapshot(snapShotIndex, snapshotBuffer.Bytes())
}

func (kv *KVServer) loadSnapshot(snapshotIndex int, snapshotData []byte) {
	kv.mu.Lock()
	r := bytes.NewBuffer(snapshotData)
	d := labgob.NewDecoder(r)
	var kvStore map[string]string
	var opResStore map[int64]LastOpResult
	if d.Decode(&kvStore) != nil || d.Decode(&opResStore) != nil {
		log.Panicf("server %v load snapshot failed %v %v", kv.me, kvStore, opResStore)
	}
	kv.kvStore = kvStore
	kv.opResStore = opResStore
	kv.lastAppliedIndex = snapshotIndex
	//notifyChanMap不用变更
	kv.mu.Unlock()
	log.Printf("server %v load snapshot success{lastApplied Index:%v}", kv.me, kv.lastAppliedIndex)
}

func (kv *KVServer) applyCommitLog() {
	for m := range kv.applyCh {
		if m.CommandValid {
			//应用信息
			opCommand := m.Command.(Op)
			kv.mu.Lock()
			//更新appliedIndex
			kv.lastAppliedIndex = m.CommandIndex
			opRes, ok := kv.opResStore[opCommand.ClientStamp]
			if !ok || opRes.OpStamp != opCommand.OpStamp {
				//执行操作
				//执行具体的操作
				commandErr := OK
				commandValue := ""
				//首先获取key值
				keyValue, ok := kv.kvStore[opCommand.Key]
				if opCommand.OpType == OpGet {
					commandValue = keyValue
					if !ok {
						commandErr = ErrNoKey
					}
				} else if opCommand.OpType == OpPut {
					kv.kvStore[opCommand.Key] = opCommand.Value
				} else {
					kv.kvStore[opCommand.Key] = keyValue + opCommand.Value
				}
				//结果记录在map中
				kv.opResStore[opCommand.ClientStamp] = LastOpResult{OpStamp: opCommand.OpStamp, Err: Err(commandErr), Value: commandValue}
				opRes = kv.opResStore[opCommand.ClientStamp]
				if opCommand.OpType != OpGet {
					log.Printf("server %v apply %v %v:%v complete, result %v:%v(prestamp:%v)", kv.me, opCommand.OpType, opCommand.ClientStamp, opCommand.OpStamp, opCommand.Key, kv.kvStore[opCommand.Key], opRes.OpStamp)
				}
			}
			notifyChan := kv.notifyChanStore[opCommand.ClientStamp]
			kv.mu.Unlock()
			//判断是否需要通知
			if notifyChan != nil {
				//首先删除
				notifyChan <- opRes
			}
			//如果超过最大大小，调用snapshot方法（不能异步，因为有可能下一个commitLog进来，快照还未完成）
			if kv.maxraftstate != -1 && kv.rf.GetLogSize() > kv.maxraftstate {
				kv.snapshotStatus()
			}
		} else if m.SnapshotValid && m.SnapshotIndex > kv.lastAppliedIndex {
			log.Printf("server %v ready to change to snapshot", kv.me)
			kv.loadSnapshot(m.SnapshotIndex, m.Snapshot)
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	lastAppliedIndex, snapshot := kv.rf.GetSnapShot()

	if len(snapshot) > 0 {
		log.Printf("server：%v start with snapshot %v", me, snapshot)
		kv.loadSnapshot(lastAppliedIndex, snapshot)
	} else {
		kv.kvStore = make(map[string]string)
		kv.opResStore = make(map[int64]LastOpResult)
		kv.lastAppliedIndex = 0
	}
	kv.notifyChanStore = make(map[int64]chan LastOpResult)
	log.Printf("server：%v start successful", me)
	//启动读取函数
	go kv.applyCommitLog()
	return kv
}
