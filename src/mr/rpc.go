package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//常量
const JOBTYPENONE = 0
const JOBTYPEMAP = 1
const JOBTYPEREDUCE = 2

//其他公用数据类型
// for sorting by key.
type ByKey []KeyValue

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
//空请求头
type EmptyArgs struct {
}

type EmptyReply struct {
}

//向coordinator注册

type RegisterWorkerReply struct {
	WorkerId  int
	ReduceNum int
}

//请求工作的请求与回复
type AskForJobArgs struct {
	WorkerId int
}

type AskForJobReply struct {
	IsOver    bool
	JobType   int
	JobNumber int
	FileList  []string
}

//返回map任务执行完成结果
type MapJobAccomplishCallBack struct {
	WokerId int
	//提供给coordinator判断哪个map任务
	MapJobNumber int
	MapJobString string
	FileList     []string
}

type ReduceJobAccomplishCallBack struct {
	WokerId int
	//提供给coordinator判断哪个map任务
	ReduceJobId int
	ReduceFile  string
}

type JobAccomplishReply struct {
	Success bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
