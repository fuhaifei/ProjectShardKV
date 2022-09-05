package shardkv

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

func init() {
	log.SetOutput(ioutil.Discard)
}

const (
	MaxWaitTime         = 500
	LoadConfigInterval  = 100
	UpdateShardInterval = 100
	GCInterval          = 50

	//shard的三种状态
	ShardNormal = "Normal"
	ShardOut    = "Out"
	ShardDelete = "Detele"
	ShardWaitIn = "WaitIn"
	ShardIn     = "In"
)

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

type ConfigOp struct {
	Config shardctrler.Config //切换新配置操作参数
}

type ShardOp struct {
	//公共参数
	OpType    string
	ConfigNum int
	//修改配置操作参数
	Config shardctrler.Config
	//添加shard操作参数
	AddShard int
	Shard    ShardData
	//删除操作参数
	DelShard int
	//确认shard操作参数
	AckShard int //迁移进入shard的参数
}

type DelShardOp struct {
	ConfigNum int //非切换配置需要使用的去重参数
	DelShard  int //迁移出删除shard的参数

	AddShard int       //迁移进入shard的参数
	Shard    ShardData //迁移进入shard的参数
}

type InShardOp struct {
	ConfigNum int       //非切换配置需要使用的去重参数
	AddShard  int       //迁移进入shard的参数
	Shard     ShardData //迁移进入shard的参数
}

type AckShardOp struct {
	ConfigNum int //非切换配置需要使用的去重参数
	AckShard  int //迁移进入shard的参数
}

type ShardData struct {
	KvStore     map[string]string
	OpResStore  map[int64]LastOpResult
	ShardStatus string
}

//临时存储执行结果
type LastOpResult struct {
	OpStamp int64  //最后一个操作的序列号
	Err     Err    //错误结果
	Value   string //返回值
}

type ShardKV struct {
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	//lab4中新加的相关参数
	dead      int32 // set by Kill()
	configing int32
	mck       *shardctrler.Clerk
	config    shardctrler.Config
	preConfig shardctrler.Config

	//lab3中存储状态的相关参数
	allShards        map[int]ShardData           //存储所有的shard相关数据
	notifyChanStore  map[int64]chan LastOpResult //用来通知返回结果的chan
	lastAppliedIndex int
}

func GzipEncode(input []byte) ([]byte, error) {
	// 创建一个新的 byte 输出流
	var buf bytes.Buffer
	// 创建一个新的 gzip 输出流

	//NoCompression      = flate.NoCompression      // 不压缩
	//BestSpeed          = flate.BestSpeed          // 最快速度
	//BestCompression    = flate.BestCompression    // 最佳压缩比
	//DefaultCompression = flate.DefaultCompression // 默认压缩比
	//gzip.NewWriterLevel()
	gzipWriter := gzip.NewWriter(&buf)
	// 将 input byte 数组写入到此输出流中
	_, err := gzipWriter.Write(input)
	if err != nil {
		_ = gzipWriter.Close()
		return nil, err
	}
	if err := gzipWriter.Close(); err != nil {
		return nil, err
	}
	// 返回压缩后的 bytes 数组
	return buf.Bytes(), nil
}

func GzipDecode(input []byte) ([]byte, error) {
	// 创建一个新的 gzip.Reader
	bytesReader := bytes.NewReader(input)
	gzipReader, err := gzip.NewReader(bytesReader)
	if err != nil {
		return nil, err
	}
	defer func() {
		// defer 中关闭 gzipReader
		_ = gzipReader.Close()
	}()
	buf := new(bytes.Buffer)
	// 从 Reader 中读取出数据
	if _, err := buf.ReadFrom(gzipReader); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (kv *ShardKV) snapshotStatus() {
	//首先将状态转化为byte数组
	snapshotBuffer := new(bytes.Buffer)
	snapshotEncoder := labgob.NewEncoder(snapshotBuffer)
	kv.mu.RLock()
	//保存kv状态和此时的历史结果（便于去重）
	if snapshotEncoder.Encode(kv.configing) != nil || snapshotEncoder.Encode(kv.preConfig) != nil || snapshotEncoder.Encode(kv.config) != nil || snapshotEncoder.Encode(kv.allShards) != nil {
		log.Panicf("server {gruop:%v, id:%v} encode failed:%v %v %v %v ", kv.gid, kv.me, snapshotEncoder.Encode(kv.configing), snapshotEncoder.Encode(kv.config), snapshotEncoder.Encode(kv.preConfig), snapshotEncoder.Encode(kv.allShards))
	}
	snapShotIndex := kv.lastAppliedIndex
	//log.Printf("server {gruop:%v, id:%v} starttoSnapshot %v  %v %v", kv.gid, kv.me, snapShotIndex, kv.config, kv.preConfig)
	kv.mu.RUnlock()

	// r := bytes.NewBuffer(snapshotBuffer.Bytes())
	// d := labgob.NewDecoder(r)
	// var allShards map[int]ShardData
	// var cognfigging int32
	// var config shardctrler.Config
	// var preConfig shardctrler.Config
	// if d.Decode(&cognfigging) != nil || d.Decode(&config) != nil || d.Decode(&preConfig) != nil || d.Decode(&allShards) != nil {
	// 	log.Panicf("server {gruop:%v, id:%v} load snapshot failed %v ", kv.gid, kv.me, allShards)
	// }
	//log.Printf("server {gruop:%v, id:%v} starttoSnapshot:%v ", kv.gid, kv.me, snapShotIndex)
	//调用快照
	compressByte, compressError := GzipEncode(snapshotBuffer.Bytes())
	if compressError != nil {
		log.Panicf("server {gruop:%v, id:%v} compress snapshot failed:%v", kv.gid, kv.me, compressError)
	}
	kv.rf.Snapshot(snapShotIndex, compressByte)
}

func (kv *ShardKV) loadSnapshot(snapshotIndex int, snapshotData []byte) {
	kv.mu.Lock()
	//log.Printf("server {gruop:%v, id:%v} ready to change snapshot %v %v ", kv.gid, kv.me, kv.lastAppliedIndex, snapshotIndex)
	if kv.lastAppliedIndex < snapshotIndex {
		snapShotDecompress, decompressError := GzipDecode(snapshotData)
		if decompressError != nil {
			log.Panicf("server {gruop:%v, id:%v} decompress snapshot failed:%v", kv.gid, kv.me, decompressError)
		}
		r := bytes.NewBuffer(snapShotDecompress)
		d := labgob.NewDecoder(r)
		var allShards map[int]ShardData
		var cognfigging int32
		var preConfig shardctrler.Config
		var config shardctrler.Config
		if d.Decode(&cognfigging) != nil || d.Decode(&preConfig) != nil || d.Decode(&config) != nil || d.Decode(&allShards) != nil {
			log.Panicf("server {gruop:%v, id:%v} load snapshot failed %v ", kv.gid, kv.me, allShards)
		}
		kv.allShards = allShards
		kv.configing = cognfigging
		kv.preConfig = preConfig
		kv.config = config
		kv.lastAppliedIndex = snapshotIndex
		//log.Printf("server {gruop:%v, id:%v} load snapshot success{lastApplied Index:%v %v} {config:%v}", kv.gid, kv.me, kv.lastAppliedIndex, snapshotIndex, config)
	} else {
		//log.Printf("server {gruop:%v, id:%v} 快照过期", kv.gid, kv.me)
	}
	//notifyChanMap不用变更
	kv.mu.Unlock()
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//提交空日志
	getOp := Op{ClientStamp: args.ClientStamp, OpStamp: args.OpStamp, OpType: OpGet, Key: args.Key}
	reply.Err, reply.Value = kv.serveClientOp(getOp)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	putOp := Op{ClientStamp: args.ClientStamp, OpStamp: args.OpStamp, OpType: args.Op, Key: args.Key, Value: args.Value}
	reply.Err, _ = kv.serveClientOp(putOp)
}

func (kv *ShardKV) serveClientOp(op Op) (Err, string) {
	// Your code here.
	var resultError Err
	resultValue := ""
	kv.mu.RLock()
	//1.判断请求是否访问错误Group
	opShardId := key2shard(op.Key)
	aimShard, ok := kv.allShards[opShardId]
	if !ok || aimShard.ShardStatus == ShardOut || aimShard.ShardStatus == ShardDelete {
		resultError = ErrWrongGroup
		kv.mu.RUnlock()
		//log.Printf("server {gruop:%v, id:%v} return reqeust %v(error wrong group: %v %v %v)", kv.gid, kv.me, op, ok, aimShard, kv.config)
		return resultError, resultValue
	}
	if aimShard.ShardStatus == ShardWaitIn {
		resultError = ErrNotReady
		kv.mu.RUnlock()
		//log.Printf("server {gruop:%v, id:%v} return reqeust %v(error not ready)", kv.gid, kv.me, op)
		return resultError, resultValue
	}
	//2.请求去重
	opRes, ok := kv.allShards[opShardId].OpResStore[op.ClientStamp]
	if ok && opRes.OpStamp == op.OpStamp {
		//需要判断OpStamp，因为访问完不能删除，有可能新请求读到老op
		resultError, resultValue = opRes.Err, opRes.Value
		kv.mu.RUnlock()
		//log.Printf("server {gruop:%v, id:%v} return reqeust %v, err:%v, value:%v(already excuted)", kv.gid, kv.me, op.OpType, resultError, resultValue)
		return resultError, resultValue
	}
	kv.mu.RUnlock()
	//2.发起日志写入请求
	kv.mu.Lock()
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		resultError = ErrWrongLeader
		//log.Printf("server {gruop:%v, id:%v} return reqeust %v, err:%v, value:%v", kv.gid, kv.me, op.OpType, resultError, resultValue)
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
				//关闭通知通道
				kv.notifyChanStore[op.ClientStamp] = nil
				kv.mu.Unlock()
				resultError = opRes.Err
				resultValue = opRes.Value
				// if op.Key == "0" {
				// 	log.Printf("server {gruop:%v, id:%v} return reqeust %v, err:%v, value:%v(key:%v, value:%v, result:%v client stamp:%v)", kv.gid, kv.me, op.OpType, resultError, resultValue, op.Key, op.Value, opRes.OpStamp, op.ClientStamp)

				// }
				return resultError, resultValue
			}
		default:
			//休眠10微妙
			time.Sleep(10 * time.Millisecond)
		}
	}
	resultError = ErrTimeOut
	//认为超时后，删除对应的chan，表示不再接收返回结果
	kv.mu.Lock()
	kv.notifyChanStore[op.ClientStamp] = nil
	kv.mu.Unlock()

	//log.Printf("server {gruop:%v, id:%v} return reqeust %v, err:%v, value:%v(time out)", kv.gid, kv.me, op.OpType, resultError, resultValue)
	return resultError, resultValue
}

func (kv *ShardKV) applyCommitLog() {
	for m := range kv.applyCh {
		if m.CommandValid {
			kv.mu.Lock()
			//更新appliedIndex
			//log.Printf("--------------- {group:%v, server:%v}CommandIndex:%v, %v", kv.gid, kv.me, kv.lastAppliedIndex, m.CommandIndex)
			if m.CommandIndex-kv.lastAppliedIndex != 1 {
				log.Printf("--------------- {group:%v, server:%v}lastAndBefore:%v, %v", kv.gid, kv.me, kv.lastAppliedIndex, m.CommandIndex)
			}
			if kv.lastAppliedIndex < m.CommandIndex {
				kv.lastAppliedIndex = m.CommandIndex
				if opCommand, ok := m.Command.(Op); ok {
					OpResult := kv.processClientOp(opCommand)
					//判断是否需要通知
					notifyChan := kv.notifyChanStore[opCommand.ClientStamp]
					kv.mu.Unlock()
					if notifyChan != nil {
						notifyChan <- OpResult
					}
				} else {
					//配置操作
					kv.processConfigOp(m.Command)
					kv.mu.Unlock()
				}
				//判断是否需要进行snapshot
				//如果超过最大大小，调用snapshot方法（不能异步，因为有可能下一个commitLog进来，快照还未完成）
				if kv.maxraftstate != -1 && kv.rf.GetLogSize() > kv.maxraftstate {
					kv.snapshotStatus()
				}
			} else {
				kv.mu.Unlock()
			}
		} else if m.SnapshotValid {
			kv.loadSnapshot(m.SnapshotIndex, m.Snapshot)
		}
	}
}

func (kv *ShardKV) processClientOp(opCommand Op) LastOpResult {
	//普通操作
	opShardId := key2shard(opCommand.Key)
	aimShard, ok := kv.allShards[opShardId]
	if !ok || aimShard.ShardStatus == ShardOut || aimShard.ShardStatus == ShardDelete {
		//log.Printf("server {gruop:%v, id:%v} (worng gruop %v)", kv.gid, kv.me, aimShard.ShardStatus)
		return LastOpResult{OpStamp: opCommand.OpStamp, Err: ErrWrongGroup, Value: ""}
	}
	if aimShard.ShardStatus == ShardWaitIn {
		return LastOpResult{OpStamp: opCommand.OpStamp, Err: ErrNotReady, Value: ""}
	}
	opRes, ok := kv.allShards[opShardId].OpResStore[opCommand.ClientStamp]
	if !ok || opRes.OpStamp < opCommand.OpStamp {
		//执行操作
		//执行具体的操作
		commandErr := OK
		commandValue := ""
		//首先获取key值
		keyValue, ok := kv.allShards[opShardId].KvStore[opCommand.Key]
		if opCommand.OpType == OpGet {
			commandValue = keyValue
			if !ok {
				commandErr = ErrNoKey
			}
		} else if opCommand.OpType == OpPut {
			kv.allShards[opShardId].KvStore[opCommand.Key] = opCommand.Value
		} else {
			kv.allShards[opShardId].KvStore[opCommand.Key] = keyValue + opCommand.Value
		}
		//结果记录在map中
		kv.allShards[opShardId].OpResStore[opCommand.ClientStamp] = LastOpResult{OpStamp: opCommand.OpStamp, Err: Err(commandErr), Value: commandValue}
	}
	return kv.allShards[opShardId].OpResStore[opCommand.ClientStamp]
}

func (kv *ShardKV) processConfigOp(commandInterface interface{}) {

	switch opCommand := commandInterface.(type) {
	case ConfigOp:
		//过滤重复的修改配置操作（因为从写入日志到日志提交存在时间差，可能重复提交日志）
		if opCommand.Config.Num > kv.config.Num {
			kv.preConfig = kv.config
			kv.config = opCommand.Config
			//根据新旧conifg,更新shard状态信息
			changingConfig := false
			for shardId, gid := range kv.preConfig.Shards {
				if gid == kv.gid && kv.config.Shards[shardId] != kv.gid {
					//1.移动走的情况
					changingConfig = true
					aimShard := kv.allShards[shardId]
					aimShard.ShardStatus = ShardOut
					kv.allShards[shardId] = aimShard
				} else if gid != kv.gid && kv.config.Shards[shardId] == kv.gid {
					//2.移动来的情况
					kv.allShards[shardId] = ShardData{ShardStatus: ShardWaitIn, KvStore: map[string]string{}, OpResStore: map[int64]LastOpResult{}}
					//当前一个配置的shard，分配为gid为0时，不需要进行shard移动
					if gid == 0 {
						kv.allShards[shardId] = ShardData{ShardStatus: ShardNormal, KvStore: map[string]string{}, OpResStore: map[int64]LastOpResult{}}
					} else {
						changingConfig = true
					}
				}
			}
			//修改切换配置
			kv.changeConfigState(changingConfig)
			//log.Printf("server {gruop:%v, id:%v} change to config{\npreConfig:%v\ncurConfig:%v\n}", kv.gid, kv.me, kv.preConfig, kv.config)
		}
	case InShardOp:
		if opCommand.ConfigNum == kv.config.Num {
			//添加shard(去重)
			if kv.allShards[opCommand.AddShard].ShardStatus == ShardWaitIn {
				kv.allShards[opCommand.AddShard] = ShardData{ShardStatus: ShardIn, KvStore: opCommand.Shard.KvStore, OpResStore: opCommand.Shard.OpResStore}
			}
			//log.Printf("server {gruop:%v, id:%v} receive shard :%v, %v(wait in ->in)", kv.gid, kv.me, opCommand.AddShard, opCommand.Shard)
		}
	case DelShardOp:
		if opCommand.ConfigNum == kv.config.Num {
			//修改shard状态为delete(去重)
			aimShard, ok := kv.allShards[opCommand.DelShard]
			if ok && aimShard.ShardStatus == ShardOut {
				aimShard.ShardStatus = ShardDelete
				kv.allShards[opCommand.DelShard] = aimShard
				//log.Printf("server {gruop:%v, id:%v} del shard(out ->delete) :%v", kv.gid, kv.me, opCommand.DelShard)
			}
		}
	case AckShardOp:
		if opCommand.ConfigNum == kv.config.Num {
			//从in状态修改为normal状态(去重)
			if kv.allShards[opCommand.AckShard].ShardStatus == ShardIn {
				aimShard := kv.allShards[opCommand.AckShard]
				aimShard.ShardStatus = ShardNormal
				kv.allShards[opCommand.AckShard] = aimShard
			}
			//log.Printf("server {gruop:%v, id:%v} ack shard :%v(in ->normal)", kv.gid, kv.me, opCommand.AckShard)
		}
	}
}

//
// 不同gruop之间交换shard的相关代码
//

func DeepCopyMap(value interface{}) interface{} {
	if valueMap, ok := value.(map[string]string); ok {
		newMap := make(map[string]string)
		for k, v := range valueMap {
			newMap[k] = v
		}
		return newMap
	} else if valueMap, ok := value.(map[int64]LastOpResult); ok {
		newMap := make(map[int64]LastOpResult)
		for k, v := range valueMap {
			newMap[k] = v
		}

		return newMap
	}

	return value
}

// type ConfigOp struct {
// 	OpType string

// 	Config   shardctrler.Config //切换新配置操作参数
// 	DelShard int                //迁移出删除shard的参数

// 	AddShard int       //迁移进入shard的参数
// 	Shard    ShardData //迁移进入shard的参数
// }

func (kv *ShardKV) callMigrateShard(shardId int, configNum int, allServers []string) {
	//首先上锁获取所有server
	//log.Printf("-server {gruop:%v, id:%v} start poll %v shard- from %v", kv.gid, kv.me, shardId, allServers[0])
	args := MigrateShardArgs{ShardId: shardId, ConfigNum: kv.config.Num}
	for _, server := range allServers {
		serverClient := kv.make_end(server)
		reply := MigrateShardReply{}

		ok := serverClient.Call("ShardKV.MigrateShardRpc", &args, &reply)
		if ok {
			if reply.Err == ErrOutDate {
				//log.Printf("server {gruop:%v, id:%v} poll shard %v failed(not change %v) RETURN", kv.gid, kv.me, shardId, allServers)
				break
			} else if reply.Err == ErrWrongLeader {
				//log.Printf("server {gruop:%v, id:%v} poll %v shard failed(wrong leader) RETURN", kv.gid, kv.me, shardId)
			} else {
				configOp := InShardOp{ConfigNum: configNum, AddShard: shardId, Shard: reply.AimShard}
				_, _, isLeader := kv.rf.Start(configOp)
				if isLeader {
					//log.Printf("server {gruop:%v, id:%v} poll %v success RETURN", kv.gid, kv.me, shardId)
				} else {
					//log.Printf("server {gruop:%v, id:%v} poll %v success, but commit failed(not leader again) RETURN", kv.gid, kv.me, shardId)
				}
				break
			}
		}
	}
}

func (kv *ShardKV) callMigrateShardAck(shardId int, configNum int, allServers []string) {
	//log.Printf("-server {gruop:%v, id:%v} start ack %v shard-, allServers%v", kv.gid, kv.me, shardId, allServers)
	args := MigrateAckArgs{ConfigNum: configNum, AckShard: shardId}
	for _, server := range allServers {
		serverClient := kv.make_end(server)
		reply := MigrateAckReply{}

		ok := serverClient.Call("ShardKV.MigrateShardAckRpc", &args, &reply)
		if ok {
			if reply.Err == ErrOutDate || reply.Err == ErrNotDeleted {
				//log.Printf("server {gruop:%v, id:%v} ack shard %v failed(not change %v config:%v)", kv.gid, kv.me, shardId, reply.Err, allServers)
				break
			} else if reply.Err == ErrWrongLeader {
				//log.Printf("server {gruop:%v, id:%v} ack %v shard failed(wrong leader)", kv.gid, kv.me, shardId)
			} else {
				configOp := AckShardOp{ConfigNum: configNum, AckShard: shardId}
				_, _, isLeader := kv.rf.Start(configOp)
				if isLeader {
					//log.Printf("server {gruop:%v, id:%v} ack %v success", kv.gid, kv.me, shardId)
				} else {
					//log.Printf("server {gruop:%v, id:%v} ack %v success, but commit failed(not leader again)", kv.gid, kv.me, shardId)
				}
				break
			}
		} else {
			//log.Printf("-server {gruop:%v, id:%v} start ack %v shard- (not ok)", kv.gid, kv.me, shardId)
		}
	}
}

func (kv *ShardKV) MigrateShardRpc(args *MigrateShardArgs, reply *MigrateShardReply) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	if args.ConfigNum > kv.config.Num {
		reply.Err = ErrOutDate
		return
	}
	//不需要考虑outdate问题，只要切换配置，则不会修改切换涉及到的shard
	reply.Err = OK
	reply.AimShard = ShardData{KvStore: DeepCopyMap(kv.allShards[args.ShardId].KvStore).(map[string]string),
		OpResStore: DeepCopyMap(kv.allShards[args.ShardId].OpResStore).(map[int64]LastOpResult)}
}

func (kv *ShardKV) MigrateShardAckRpc(args *MigrateAckArgs, reply *MigrateAckReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	if args.ConfigNum > kv.config.Num {
		reply.Err = ErrOutDate
		return
	}
	aimShard, ok := kv.allShards[args.AckShard]
	if kv.config.Num > args.ConfigNum || !ok || aimShard.ShardStatus == ShardDelete {
		reply.Err = OK
		//log.Printf("server {gruop:%v, id:%v} already delete shard %v(%v)", kv.gid, kv.me, args.AckShard, ok)
		return
	}
	//提交修改状态为delete的日志
	configOp := DelShardOp{ConfigNum: args.ConfigNum, DelShard: args.AckShard}
	_, _, logSuccess := kv.rf.Start(configOp)
	if logSuccess {
		reply.Err = ErrNotDeleted
		//log.Printf("server {gruop:%v, id:%v} write log :%v status to delete success(wait to delete)", kv.gid, kv.me, args.AckShard)
	} else {
		reply.Err = ErrWrongLeader
		//log.Printf("server {gruop:%v, id:%v} write log :%v status to delete(failed not Leader)", kv.gid, kv.me, args.AckShard)
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//

func (kv *ShardKV) loadConfig() {
	//周期性加载
	kv.mu.RLock()
	_, isLeader := kv.rf.GetState()
	if isLeader && !kv.isConfiging() {
		kv.mu.RUnlock()
		//当且仅当为leader时进行配置拉取且非切换配置时
		curConfig := kv.mck.Query(kv.config.Num + 1)
		kv.mu.RLock()
		if curConfig.Num > kv.config.Num {
			//提交config线程
			op := ConfigOp{Config: curConfig}
			_, _, isLeader = kv.rf.Start(op)
			//log.Printf("server {gruop:%v, id:%v} change config start is leader?:%v", kv.gid, kv.me, isLeader)
		}
	}
	kv.mu.RUnlock()
}

func (kv *ShardKV) updateShardState(updateFunc func(int, int, []string), chaeckStatus string) {
	kv.mu.RLock()
	_, isLeader := kv.rf.GetState()
	//如果在配置
	if kv.isConfiging() && isLeader {
		var wg sync.WaitGroup
		//log.Printf("server {gruop:%v, id:%v} preConfig:%v", kv.gid, kv.me, kv.preConfig)
		//log.Printf("server {gruop:%v, id:%v} curConfig:%v", kv.gid, kv.me, kv.config)
		for shardId, shard := range kv.allShards {
			if shard.ShardStatus == chaeckStatus {
				wg.Add(1)
				go func(shardId int, configNum int, allServers []string) {
					defer wg.Done()
					updateFunc(shardId, configNum, allServers)
				}(shardId, kv.config.Num, kv.preConfig.Groups[kv.preConfig.Shards[shardId]])
			}
		}
		//释放锁，并等待
		kv.mu.RUnlock()
		wg.Wait()
		kv.mu.RLock()
		completeFlag := true
		for _, shard := range kv.allShards {
			if shard.ShardStatus != ShardNormal && shard.ShardStatus != ShardDelete {
				//log.Printf("server {gruop:%v, id:%v} change configging:%v, %v", kv.gid, kv.me, shardId, shard.ShardStatus)
				completeFlag = false
			}
		}
		kv.mu.RUnlock()
		if completeFlag {
			kv.changeConfigState(false)
		}
		//log.Printf("server {gruop:%v, id:%v} change configging:%v %v", kv.gid, kv.me, completeFlag, kv.config.Num)
	} else {
		kv.mu.RUnlock()
	}
}

func (kv *ShardKV) garbageCollect() {
	kv.mu.Lock()
	for shardId, shard := range kv.allShards {
		if shard.ShardStatus == ShardDelete {
			//删除对应状态的shard
			delete(kv.allShards, shardId)
		}
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) backRoutine(operation func(), interval int) {
	for !kv.killed() {
		//执行具体操作
		operation()
		time.Sleep(time.Duration(interval) * time.Millisecond)
	}
}

func (kv *ShardKV) isConfiging() bool {
	configing := atomic.LoadInt32(&kv.configing)
	return configing == 1
}

func (kv *ShardKV) changeConfigState(ifConfig bool) {
	if ifConfig {
		atomic.StoreInt32(&kv.configing, 1)
	} else {
		atomic.StoreInt32(&kv.configing, 0)
	}
}

func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(ConfigOp{})
	labgob.Register(DelShardOp{})
	labgob.Register(InShardOp{})
	labgob.Register(AckShardOp{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// You may need initialization code here.
	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	//快照加载
	lastAppliedIndex, snapshot := kv.rf.GetSnapShot()
	if len(snapshot) > 0 {
		//log.Printf("server：{gruop:%v, id:%v}  start with snapshot", gid, me)
		kv.loadSnapshot(lastAppliedIndex, snapshot)
	} else {
		kv.allShards = make(map[int]ShardData)
		kv.configing = 0
		kv.config = shardctrler.Config{}
		kv.lastAppliedIndex = 0
	}
	kv.notifyChanStore = make(map[int64]chan LastOpResult)
	//log.Printf("server：{gruop:%v, id:%v}  start successful kv.lastAppliedIndex:%v, maxRaftState:%v", gid, me, kv.lastAppliedIndex, kv.maxraftstate)

	//启动日志读取
	go kv.applyCommitLog()

	//启动更新配置函数
	go kv.backRoutine(kv.loadConfig, LoadConfigInterval)
	//启动垃圾回收线程
	go kv.backRoutine(kv.garbageCollect, GCInterval)
	//启动拉取配置切换中需要shard的线程
	go kv.backRoutine(func() { kv.updateShardState(kv.callMigrateShard, ShardWaitIn) }, UpdateShardInterval)
	//启动确认收到对应shard的线程
	go kv.backRoutine(func() { kv.updateShardState(kv.callMigrateShardAck, ShardIn) }, UpdateShardInterval)
	return kv
}
