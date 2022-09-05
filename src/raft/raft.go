package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"math"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"

	//自己添加的依赖

	"log"
	"time"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

// func init() {
// 	log.SetOutput(ioutil.Discard)
// }

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
// 常量定义区（8/2）
// 定义raft worker的状态常量（8/2）
const STATUS_FOLLOWER = 0
const STATUS_CAND = 1
const STATUS_LEADER = 2

//定义选举超时时间
const MIN_ELECTION_OUT = 250 //最小选举超时时间
const MAX_ELECTION_OUT = 400 //最大选举超时时间

//（8/3）
const IDLE_INTERVAL_TIME = 100 //发送空appenentry的时间间隔

//（8/5）
const LEADER_COMMIT_CHECK_INTERVAL = 20 //检测是否推进commitIndex时间间隔

//(8/8)
const SNAPSHOT_THRESHOLD = 1000 //进行快照的阈值

type LogEntry struct {
	Term int
	//记录当前日志的index
	Index   int
	Command interface{}
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//lab2a选举实现（8/2）
	//1.自己设计的状态部分
	peer_status      int   //阶段当前状态：follower/candidate/leader
	election_timeout int64 //记录触发选举超时的时间
	//2.一般状态
	currentTerm int        //当前逻辑时钟号
	voteFor     int        //当前任期内的投票号
	log         []LogEntry //日志列表
	//3.维护日志提交状态
	commitIndex int           //已提交的最大index
	lastApplied int           //已经应用到状态机的最大index
	applyCh     chan ApplyMsg //提交message后传输到应用层的管道
	applyCond   sync.Cond     //提醒提交线程的条件变量
	//4.server独有的全局状态
	nextIndex    []int     //发送给follower的下一个日志index
	mathchIndex  []int     //认为备份成功的最大index
	lastSendTime int64     //上一次发送appendEntry的时间
	leaderCond   sync.Cond //leader的条件变量，成为leader用来同步leader操作

	//For snapshot
	snapshotMsgSending int32
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	//判断状态代码（8/2）
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.peer_status == STATUS_LEADER {
		isleader = true
	} else {
		isleader = false
	}
	term = rf.currentTerm
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
//工具函数
func (rf *Raft) getRaftState() []byte {
	persist_buffer := new(bytes.Buffer)
	persist_encoder := labgob.NewEncoder(persist_buffer)
	persist_encoder.Encode(rf.currentTerm)
	persist_encoder.Encode(rf.voteFor)
	persist_encoder.Encode(rf.log)
	return persist_buffer.Bytes()
}

func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	//持久化代码（8.7）
	rf.persister.SaveRaftState(rf.getRaftState())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	//持久化代码（8.7）
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var voeteFor int
	var peer_log []LogEntry
	if d.Decode(&term) != nil || d.Decode(&voeteFor) != nil || d.Decode(&peer_log) != nil {
		log.Panic("加载失败")
	} else {
		rf.currentTerm = term
		rf.voteFor = voeteFor
		rf.log = peer_log
	}
}

//
//获取当前持久化状态的大小
//
func (rf *Raft) GetLogSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) GetSnapShot() (int, []byte) {
	return rf.log[0].Index, rf.persister.ReadSnapshot()
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	//不需要实现该接口
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.

func (rf *Raft) isSendingSnapshot() bool {
	z := atomic.LoadInt32(&rf.snapshotMsgSending)
	return z == 1
}

func (rf *Raft) startSendingSnapshot() {
	atomic.AddInt32(&rf.snapshotMsgSending, 1)
}

func (rf *Raft) finnishSendingSnapshot() {
	atomic.AddInt32(&rf.snapshotMsgSending, -1)
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.log[0].Index {
		//若要求备份index小于伪log的index，则不需要备份
		return
	}
	//这里的lastApplied和commitindex一定大于index

	rf.lastApplied -= index - rf.log[0].Index
	rf.commitIndex -= index - rf.log[0].Index
	if rf.peer_status == STATUS_LEADER {
		//修改nextIndex小于目标Index的
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				rf.nextIndex[i] -= index - rf.log[0].Index
				rf.mathchIndex[i] -= index - rf.log[0].Index
			}
		}
		//log.Printf("pper:%d Snapshot index:%d nextIndex:%v, matchIndex:%v", rf.me, index, rf.nextIndex, rf.mathchIndex)
	}

	//留下被截断的最后一个log，作为伪log头(复制方式，避免影响垃圾回收)
	rf.log = append([]LogEntry{}, rf.log[index-rf.log[0].Index:]...)
	rf.log[0].Command = nil
	//移动index
	rf.persister.SaveStateAndSnapshot(rf.getRaftState(), snapshot)
	//log.Printf("pper:%d Snapshot index:%d lastApplied:%d, commitIndex:%d len log:%v", rf.me, index, rf.lastApplied, rf.commitIndex, len(rf.log))
}

type InstallSnapshotArgs struct {
	//不实现分段发送功能
	Term              int    //leader节点的term号
	LeaderId          int    //leader的id号码（目前没用）
	LastIncludedIndex int    //快照中最后一个日志的index
	LastIncludedTerm  int    //快照中最后一个日志的term
	Data              []byte //快照数据
}

type InstallSnapshotReply struct {
	Term int //peer的term用来判断当前leader是否过期
}

func (rf *Raft) InstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	//TODO
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	//判断是否过时，如果过时直接返回
	if args.Term < rf.currentTerm {
		return
	}
	//首先更新选举超时时间
	rf.election_timeout = getRandElectionTimeout()
	//若当前term过低，直接修改状态
	if args.Term > rf.currentTerm {
		rf.upToDate(args.Term)
	}
	//如果commitIndex > 发送的lastlogIndex，不需要重新修改备份
	if rf.commitIndex+rf.log[0].Index >= args.LastIncludedIndex {
		return
	}

	//截断log，更新snapshot
	//按照一致性保证
	//1.首先更新lastapplied和commitIndex
	//log.Printf("peer:%v installsnapshot:%v, lastApplied:%v, commitIndex:%v,firstlog:%v", rf.me, args.LastIncludedIndex, rf.lastApplied, rf.commitIndex, rf.log[0].Index)
	//如果lastApplied 或者 commitIndex 小于 args.LastIncludedIndex
	if rf.commitIndex+rf.log[0].Index < args.LastIncludedIndex {
		//log.Printf("peer %v:%v %v", rf.me, rf.commitIndex+rf.log[0].Index, args.LastIncludedIndex)
		rf.commitIndex = 0
		rf.lastApplied = 0
	}
	if rf.lastApplied+rf.log[0].Index < args.LastIncludedIndex {
		//log.Printf("peer %v:apply %v %v", rf.me, rf.lastApplied+rf.log[0].Index, args.LastIncludedIndex)
		rf.lastApplied = 0
	}
	//2.寻找与目标相同的index
	lastIncludedIndexInLog := args.LastIncludedIndex - rf.log[0].Index
	//3.截断日志
	if lastIncludedIndexInLog < len(rf.log) {
		rf.log = append([]LogEntry{}, rf.log[lastIncludedIndexInLog:]...)
		// rf.log[0].Index = args.LastIncludedIndex index不可能出错
		rf.log[0].Term = args.LastIncludedTerm
		rf.log[0].Command = nil
	} else {
		rf.log = []LogEntry{{Index: args.LastIncludedIndex, Term: args.LastIncludedTerm}}
	}
	//保存状态
	rf.persister.SaveStateAndSnapshot(rf.getRaftState(), args.Data)
	//写入applych
	rf.startSendingSnapshot()
	go func() {
		rf.applyCh <- ApplyMsg{CommandValid: false, SnapshotValid: true, Snapshot: args.Data,
			SnapshotIndex: args.LastIncludedIndex, SnapshotTerm: args.LastIncludedTerm}
		rf.finnishSendingSnapshot()
	}()
}

func (rf *Raft) sendSnapShot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	if ok {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.upToDate(reply.Term)
		} else {
			//修改nextIndex为目标值
			rf.nextIndex[server] = 1
			rf.mathchIndex[server] = 0
		}
		rf.mu.Unlock()
	}
	return ok
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	//lab2a(8/2)
	CandidateTerm int //candidate的term
	CandidateId   int //candidate的id
	LastLogTerm   int //candidate的最新日志term
	LastLogIndex  int //candidate的最新日志index
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).

	//Lab2a (8/2)
	Term        int  //返回term,用来判断当前server是否超时
	VoteGranted bool // 是否投票，即true或false
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	//Lab2a(8/2) 收到投票请求
	reply.VoteGranted = true
	//若接收到的投票请求中的term大于当前term，直接修改为follower
	rf.mu.Lock()

	//log.Printf("peer %v term:%v voteFor:%v receive request vote form %v term %v,fromlog{term:%v index:%v} curlog{Term %vindex: %v}",
	//	rf.me, rf.currentTerm, rf.voteFor, args.CandidateId, args.CandidateTerm, args.LastLogTerm, args.LastLogIndex, rf.log[len(rf.log)-1].Term, rf.log[len(rf.log)-1].Index)
	defer rf.mu.Unlock()
	need_persist := false
	if rf.currentTerm < args.CandidateTerm {
		rf.upToDate(args.CandidateTerm)
		need_persist = true
	}
	//是否已经为其他candidate投票，当前更新（最后一个日志的term>发送term || term相同,log长度更长）
	if rf.currentTerm > args.CandidateTerm || (rf.currentTerm == args.CandidateTerm && rf.voteFor != args.CandidateId && rf.voteFor != -1) ||
		rf.log[len(rf.log)-1].Term > args.LastLogTerm || (rf.log[len(rf.log)-1].Term == args.LastLogTerm && rf.log[len(rf.log)-1].Index > args.LastLogIndex) {
		reply.VoteGranted = false
	} else {
		rf.voteFor = args.CandidateId
		rf.election_timeout = getRandElectionTimeout()
		need_persist = true
	}
	reply.Term = rf.currentTerm
	if need_persist {
		//持久化操作(8.7)
		rf.persist()
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//工具函数构建投票请求参数
func (rf *Raft) constructVoteRequest() RequestVoteArgs {
	var vote_requst RequestVoteArgs
	vote_requst.CandidateId = rf.me
	rf.mu.Lock()
	vote_requst.CandidateTerm = rf.currentTerm
	vote_requst.LastLogIndex = rf.log[len(rf.log)-1].Index
	vote_requst.LastLogTerm = rf.log[len(rf.log)-1].Term
	rf.mu.Unlock()
	return vote_requst
}

func (rf *Raft) startElection(election_timeout int64) {
	//log.Printf("raft peer %d election timeout and change to candidate", rf.me)
	//开启投票
	var vote_lock sync.Mutex
	var finish_lock sync.Mutex
	var vote_number int32 = 1
	var finished_number int32 = 1
	//初始化请求参数对象
	vote_requst := rf.constructVoteRequest()
	for i := 0; i < len(rf.peers); i++ {
		//遍历其他的client,发送请求投票rpc
		if i != rf.me {
			go func(aimServer int) {
				var vote_reply RequestVoteReply
				//不断发送请求直到:超时、者成功响应、当前状态非candidate
				ok := rf.sendRequestVote(aimServer, &vote_requst, &vote_reply)
				//若请求响应返回Term大于当前term
				if ok {
					rf.mu.Lock()
					if rf.peer_status == STATUS_CAND && rf.currentTerm == vote_requst.CandidateTerm && time.Now().UnixMilli() < election_timeout {
						if vote_reply.Term > rf.currentTerm {
							rf.upToDate(vote_reply.Term)
							//持久化状态(8.7)
							rf.persist()
						} else {
							//如果返回结果为true，票数+1
							if vote_reply.VoteGranted {
								vote_lock.Lock()
								vote_number += 1
								vote_lock.Unlock()
							}
						}
					}
					rf.mu.Unlock()
				}
				//退出记录
				finish_lock.Lock()
				finished_number += 1
				finish_lock.Unlock()
			}(i)
		}
	}
	//主线程不断判断是否满足条件（自旋锁）
	for {
		vote_lock.Lock()
		finish_lock.Lock()
		if int(finished_number) == len(rf.peers) || int(vote_number) > len(rf.peers)/2 {
			vote_lock.Unlock()
			finish_lock.Unlock()
			break
		}
		vote_lock.Unlock()
		finish_lock.Unlock()
		rf.mu.Lock()
		//超时或者状态变更
		if time.Now().UnixMilli() >= election_timeout || rf.currentTerm != vote_requst.CandidateTerm {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()

		//休息10毫秒
		time.Sleep(time.Millisecond * 10)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//若当前状态为follower，直接返回
	if rf.currentTerm != vote_requst.CandidateTerm || rf.peer_status != STATUS_CAND {
		//log.Printf("raft peer %d election failed, beacause status is changed {currentTerm:%v, startTerm:%v}", rf.me, rf.currentTerm, vote_requst.CandidateTerm)
		return
	}
	if int(vote_number) > len(rf.peers)/2 { //判断是否成为leader
		rf.peer_status = STATUS_LEADER
		//初始化leader状态
		rf.nextIndex = make([]int, len(rf.peers))
		rf.mathchIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			//初始化为日志长度
			rf.nextIndex[i] = len(rf.log)
		}
		//启动heartbeat进程
		go rf.heartsbeats()
		//唤醒等待的commit线程
		rf.leaderCond.L.Lock()
		rf.leaderCond.Signal()
		rf.leaderCond.L.Unlock()
		//log.Printf("raft peer %d become leader", rf.me)
	} else {
		//选举失败，重置选举超时时间（把机会让给别的结点W）
		rf.election_timeout = getRandElectionTimeout()
	}
}

// 定义发送日志代码块(8/3)
type AppendEntryArgs struct {
	Term         int        //leader的term
	LeaderId     int        //用来给客户端重定向到leader
	PreLogIndex  int        //上一条日志的index
	PreLogTerm   int        //上一条日志的term
	Entries      []LogEntry //多条发送日志信息
	LeaderCommit int        //leader的提交id
}

type AppendEntryReply struct {
	Term    int  //返回的term
	Success bool //是否成功写入
	XTerm   int  //冲突日志的term
	XIndex  int  //follower中属于冲突日志中，
	XLen    int  //follower日志长度
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	//首先设置为false
	reply.Success = false
	reply.XLen = -1
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
	}()
	reply.Term = rf.currentTerm
	//判断是否过时，如果过时直接返回
	if args.Term < rf.currentTerm || args.PreLogIndex < rf.log[0].Index {
		return
	}
	need_persist := false
	//首先更新选举超时时间
	rf.election_timeout = getRandElectionTimeout()
	//修改状态
	if args.Term > rf.currentTerm {
		rf.upToDate(args.Term)
		need_persist = true
	}
	//log.Printf("raft peer %d receive {firstIndex %v len log:%v) with preLogTerm %v{fisrtIndex:%v, len log:%v})", rf.me, args.PreLogIndex, len(args.Entries), args.PreLogTerm, rf.log[0].Index, len(rf.log))
	//若日志不一致（没有对应index日志 或 对应index日志term不相等）
	preLogIndexInLog := args.PreLogIndex - rf.log[0].Index
	//log.Printf("{firstIndex %v len log:%v) {fisrtIndex:%v, len log:%v}) preLogIndexInLog:%v", args.PreLogIndex, len(args.Entries), rf.log[0].Index, len(rf.log), preLogIndexInLog)
	if len(rf.log) < preLogIndexInLog+1 || (rf.log[preLogIndexInLog].Term != args.PreLogTerm) {
		if len(rf.log) < preLogIndexInLog+1 {
			//返回下一个
			reply.XLen = len(rf.log) + rf.log[0].Index
		} else {
			//再follower日志中寻找第一个与冲突日志相同的日志项目,
			reply.XTerm = rf.log[preLogIndexInLog].Term
			for reply.XIndex = preLogIndexInLog; reply.XIndex >= 0 && rf.log[reply.XIndex].Term == reply.XTerm; reply.XIndex-- {
			}
			//转化为真实index
			reply.XIndex += rf.log[0].Index + 1
		}
		//log.Printf("raft peer %d reject log %v with preLogTerm %v(leader inconsistent{term:%v})", rf.me, args.PreLogIndex+1, args.PreLogTerm, args.Term)
		return
	}
	//5.进行日志更新(判断是否为nil,排除空entries)
	reply.Success = true
	//判断是否有日志冲突
	for i := 0; i < len(args.Entries) && i+preLogIndexInLog+1 < len(rf.log); i++ {
		if rf.log[i+preLogIndexInLog+1].Term != args.Entries[i].Term {
			//截断不相等日志区域（使用复制方式，避免内存无法释放）
			rf.log = append([]LogEntry{}, rf.log[:i+preLogIndexInLog+1]...)
		}
	}
	//若剩余不冲突日志数量小于待添加日志数量,则拼接缺失的日志
	if len(rf.log) < preLogIndexInLog+len(args.Entries)+1 {
		rf.log = append(rf.log, args.Entries[len(rf.log)-preLogIndexInLog-1:]...)
		need_persist = true
	}
	//更新commitIndex
	if args.LeaderCommit-rf.log[0].Index > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit - rf.log[0].Index
		if rf.commitIndex > len(rf.log)-1 {
			rf.commitIndex = len(rf.log) - 1
		}
	}

	//持久化状态(8.7)
	if need_persist {
		rf.persist()
	}

	if len(args.Entries) != 0 {
		//log.Printf("raft peer %d receive log %v with term %v command %v ({leader term:%v}) new commitIndex:%v, leader commit:%v", rf.me, rf.log[len(rf.log)-1].Index,
		//	rf.log[len(rf.log)-1].Term, rf.log[len(rf.log)-1].Command, args.Term, rf.commitIndex, args.LeaderCommit)
	}
	if rf.lastApplied < rf.commitIndex {
		rf.applyCond.L.Lock()
		rf.applyCond.Signal()
		rf.applyCond.L.Unlock()
	}
}

//二分查找工具函数（找到=目标term的）
func binarySearchLast(aimTerm int, allLog []LogEntry) int {
	left := 0
	right := len(allLog) - 1
	for left <= right {
		middle := (left + right) / 2
		if allLog[middle].Term <= aimTerm {
			left = middle + 1
		} else {
			right = middle - 1
		}
	}
	return right
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	//log.Printf("leader:%v send appenEntry to %v, value:%v %v", rf.me, server, args.PreLogIndex, len(args.Entries))
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		//如果当前结点的状态未变更(避免延迟返回)
		if rf.currentTerm == args.Term && rf.nextIndex[server]-1 >= 0 && args.PreLogIndex == rf.log[rf.nextIndex[server]-1].Index {
			if !reply.Success {
				//是否由于当前leader已过期
				if rf.currentTerm < reply.Term {
					rf.upToDate(reply.Term)
					//持久化状态(8.7)
					rf.persist()
				} else {
					//是不是leader都可以修改当前
					if reply.XLen != -1 {
						rf.nextIndex[server] = reply.XLen - rf.log[0].Index
					} else {
						aim_index := binarySearchLast(reply.XTerm, rf.log)
						if aim_index >= 0 && rf.log[aim_index].Term == reply.XTerm {
							//此时的nextIndex可能指向伪logentry头
							rf.nextIndex[server] = aim_index
						} else {
							rf.nextIndex[server] = reply.XIndex - rf.log[0].Index
						}
					}
					//log.Printf("leader:%v, new nextIndex", rf.nextIndex[server])
				}
			} else {
				//成功的话需要修改状态
				rf.nextIndex[server] = args.PreLogIndex + len(args.Entries) + 1 - rf.log[0].Index
				//nextIndex的前一位
				rf.mathchIndex[server] = rf.nextIndex[server] - 1
				//log.Printf("leader:%v appenEntry to %v success {lastEntryIndex:%v, newNextIndex:%v}", rf.me, server, args.PreLogIndex+len(args.Entries), rf.nextIndex[server]+rf.log[0].Index)
			}
		}
	}
	return ok
}

//抽取出的广播AppendEntry方法
func (rf *Raft) broadcastAppendEntry(isHeartbeat bool) {
	rf.mu.Lock()
	if rf.peer_status != STATUS_LEADER {
		rf.mu.Unlock()
		return
	}
	cur_term := rf.currentTerm
	//更新发送时间
	rf.lastSendTime = time.Now().UnixMilli()
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.mu.Lock()
			if cur_term != rf.currentTerm {
				rf.mu.Unlock()
				return
			}
			//判断perlogIndex是否已经存储在日志中（小于伪头entryindex）
			if rf.nextIndex[i] <= 0 {
				//调用发送快照接口
				args := InstallSnapshotArgs{Term: rf.currentTerm, LeaderId: rf.me,
					LastIncludedIndex: rf.log[0].Index, LastIncludedTerm: rf.log[0].Term, Data: rf.persister.snapshot}
				reply := InstallSnapshotReply{}
				go rf.sendSnapShot(i, &args, &reply)
			} else {
				args := AppendEntryArgs{LeaderId: rf.me, Entries: rf.log[rf.nextIndex[i]:], Term: rf.currentTerm,
					PreLogIndex: rf.log[rf.nextIndex[i]-1].Index, PreLogTerm: rf.log[rf.nextIndex[i]-1].Term, LeaderCommit: rf.log[rf.commitIndex].Index}
				reply := AppendEntryReply{}
				go rf.sendAppendEntry(i, &args, &reply)
			}
			rf.mu.Unlock()
		}
	}
}

// leader的heartbeat操作方法
func (rf *Raft) heartsbeats() {

	for !rf.killed() {
		rf.mu.Lock()
		if rf.peer_status != STATUS_LEADER {
			rf.mu.Unlock()
			break
		}
		lastSendTime := rf.lastSendTime
		rf.mu.Unlock()
		if time.Now().UnixMilli()-lastSendTime >= IDLE_INTERVAL_TIME {
			rf.broadcastAppendEntry(true)
		}
		rf.mu.Lock()
		lastSendTime = rf.lastSendTime
		rf.mu.Unlock()
		//休眠到超时（未触发，休息到触发，否则休息一个interval）
		time.Sleep(time.Duration(math.Min(IDLE_INTERVAL_TIME, float64(IDLE_INTERVAL_TIME+lastSendTime-time.Now().UnixMilli()))) * time.Millisecond)
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).
	//上锁
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.peer_status == STATUS_LEADER {
		isLeader = true
		//index为上一个log的index+1(头日志的index为0)
		index = rf.log[len(rf.log)-1].Index + 1
		rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Index: index, Command: command})
		//持久化状态
		rf.persist()
		//log.Printf("leader %d send logentry{term:%v, index:%v, command:%v}", rf.me, rf.currentTerm, index, command)
		go rf.broadcastAppendEntry(false)
	}

	term = rf.currentTerm
	return index, term, isLeader
}

//周期性复制日志操作，只有leader执行
func (rf *Raft) checkCommit() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.peer_status != STATUS_LEADER {
			rf.mu.Unlock()
			rf.leaderCond.L.Lock()
			rf.leaderCond.Wait()
			rf.leaderCond.L.Unlock()
		} else {
			rf.mu.Unlock()
		}
		//开始周期性检查，是否能增加commitIndex
		for !rf.killed() {
			time.Sleep(LEADER_COMMIT_CHECK_INTERVAL * time.Millisecond)
			rf.mu.Lock()
			if rf.killed() || rf.peer_status != STATUS_LEADER {
				rf.mu.Unlock()
				break
			}
			//首先获得所有的matchIndex并进行排序，获取中位数
			var all_match []int
			all_match = append(all_match, rf.log[len(rf.log)-1].Index) //首先添加自己的matchindex,即最后一个
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					all_match = append(all_match, rf.mathchIndex[i])
				}
			}
			sort.Ints(all_match)
			mid_match := all_match[(len(all_match)-1)/2]
			//只有在mid_match大于commitIndex且指向的日志term=当前term时，更新commitIndex
			if mid_match > rf.commitIndex && rf.log[mid_match].Term == rf.currentTerm {
				rf.commitIndex = mid_match
			}
			if rf.lastApplied < rf.commitIndex {
				rf.applyCond.L.Lock()
				rf.applyCond.Signal()
				rf.applyCond.L.Unlock()
			}
			//log.Printf("leader %d update commit index to %v, %v, %v, midmatch:%d", rf.me, rf.commitIndex, rf.nextIndex, rf.mathchIndex, mid_match)
			rf.mu.Unlock()
		}
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	// Your code here to check if a leader election should
	// be started and to randomize sleeping time using
	// time.Sleep()

	//选举触发实现（8/2）
	for !rf.killed() {
		//休眠到超时时间点
		rf.mu.Lock()
		election_timeout := rf.election_timeout
		rf.mu.Unlock()
		if election_timeout > time.Now().UnixMilli() {
			time.Sleep(time.Duration(election_timeout-time.Now().UnixMilli()) * time.Millisecond)
		}
		rf.mu.Lock()
		//判断是否发起选举
		if !rf.killed() && rf.peer_status != STATUS_LEADER && rf.election_timeout <= time.Now().UnixMilli() {
			//修改当前状态，进入candidate状态/重新发起选举
			if rf.election_timeout <= time.Now().UnixMilli() {
				rf.currentTerm++
				rf.voteFor = rf.me
				rf.peer_status = STATUS_CAND
				rf.election_timeout = getRandElectionTimeout() + 10
				//调用持久化操作(8.7)
				rf.persist()
				//启动选举线程
				go rf.startElection(rf.election_timeout - 10)
			}
		} else if rf.election_timeout <= time.Now().UnixMilli() {
			//如果为leader,则延长超时时间（8/3）
			rf.election_timeout = getRandElectionTimeout()
		}
		rf.mu.Unlock()
	}
}

//周期性执判断是否需要提交日志
func (rf *Raft) applyEntry() {

	for !rf.killed() {
		rf.mu.Lock()
		if rf.lastApplied == rf.commitIndex {
			rf.mu.Unlock()
			rf.applyCond.L.Lock()
			rf.applyCond.Wait()
			rf.applyCond.L.Unlock()
		} else {
			rf.mu.Unlock()
		}
		//首先修改lastapplied
		rf.mu.Lock()
		var applyEntries []LogEntry
		//log.Printf("peer:%v try to applyEntry，获取锁", rf.me)
		if rf.commitIndex > rf.lastApplied {
			applyEntries = append(applyEntries, rf.log[rf.lastApplied+1:rf.commitIndex+1]...)
			rf.lastApplied = rf.commitIndex
			//log.Printf("peer %v update lastApplied to %v", rf.me, rf.lastApplied+rf.log[0].Index)
		}
		rf.mu.Unlock()
		//log.Printf("peer:%v try to applyEntry，释放锁", rf.me)
		//再发送信息
		if len(applyEntries) > 0 {
			for rf.isSendingSnapshot() {
				time.Sleep(5 * time.Millisecond)
			}
			for i := 0; i < len(applyEntries); i++ {
				rf.applyCh <- ApplyMsg{CommandValid: true, SnapshotValid: false, Command: applyEntries[i].Command, CommandIndex: applyEntries[i].Index}
			}
		}
		//log.Printf("peer:%v try to applyEntry，完成发送", rf.me)
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	//lab2a 初始化状态（8/3）
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.log = make([]LogEntry, 1) //不包含日志时的长度为1,first index = 1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	var applyCondMutex sync.Mutex
	rf.applyCond = sync.Cond{L: &applyCondMutex}
	//快照状态
	rf.snapshotMsgSending = 0
	//初始化自定义状态
	rf.peer_status = STATUS_FOLLOWER
	var leaderCondMutex sync.Mutex
	rf.leaderCond = sync.Cond{L: &leaderCondMutex}
	//记录预期的超时时间
	rf.election_timeout = getRandElectionTimeout()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	//启动两个后台处理线程，一个为leader的appendEntry线程，一个为所有raft进程的apply线程
	go rf.checkCommit()
	go rf.applyEntry()

	// start ticker goroutine to start elections
	//log.Printf("raft peer %d of %d total peer, if dead %d", rf.me, len(rf.peers), rf.dead)
	go rf.ticker()

	return rf
}

//工具函数区域

func getRandElectionTimeout() int64 {
	return time.Now().UnixMilli() + rand.Int63n(MAX_ELECTION_OUT-MIN_ELECTION_OUT) + MIN_ELECTION_OUT
}

func (rf *Raft) upToDate(newTerm int) {
	if rf.currentTerm < newTerm {
		rf.currentTerm = newTerm
		rf.voteFor = -1
		rf.peer_status = STATUS_FOLLOWER
	}
}
