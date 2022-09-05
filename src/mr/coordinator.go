package mr

import (
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

func init() {
	log.SetOutput(ioutil.Discard)
}

//常量,任务的三个状态
const JOBIDLE = 0
const JOBBUSY = 1
const JOBCOMPLETED = 2

type Coordinator struct {
	// Your definitions here.
	totalMapNumber    int
	totalReduceNumber int
	//1. workerId分配
	workerCountLock sync.Mutex
	numWorkers      int
	//2. 记录map和reduce状态相关map和锁
	mapJobNumberLock    sync.RWMutex
	numMap              int
	reduceJobNumberLock sync.RWMutex
	numReduce           int
	//作业编号，作业状态，分配workerid
	mapJobStatus map[string]*[3]int
	mapJobLocks  map[string]*sync.RWMutex
	//作业状态，分配workerid
	reduceJobStatus map[int]*[3]int
	reduceJobLocks  map[int]*sync.RWMutex
	//3. 记录Map任务输出
	reduceInputsLock sync.Mutex
	reduceInputs     [][]string
}

//自定义操作方法

// Your code here -- RPC handlers for the worker to call.
//我的自定义RPC方法
//1. worker注册，获得workerId
func (c *Coordinator) RegisterWorker(args *EmptyArgs, reply *RegisterWorkerReply) error {
	c.workerCountLock.Lock()
	defer c.workerCountLock.Unlock()
	c.numWorkers++
	reply.WorkerId = c.numWorkers
	reply.ReduceNum = c.totalReduceNumber
	return nil
}

//2.分配任务
func (c *Coordinator) AllocateMapJob(workerId int) (int, string) {
	var allocateNumber int
	var allocateFile string
	if c.numMap > 0 {
		for mapJob := range c.mapJobStatus {
			if c.numMap == 0 {
				break
			}
			if c.mapJobStatus[mapJob][1] == JOBIDLE {
				//首先获取当前任务的写锁
				c.mapJobLocks[mapJob].Lock()
				defer c.mapJobLocks[mapJob].Unlock()
				//再次判断，避免死锁
				if c.mapJobStatus[mapJob][1] == JOBIDLE {
					//修改状态为 JOBBUSY处理中
					(*c.mapJobStatus[mapJob])[1] = JOBBUSY
					(*c.mapJobStatus[mapJob])[2] = workerId
					allocateNumber = (*c.mapJobStatus[mapJob])[0]
					allocateFile = mapJob
					return allocateNumber, allocateFile
				}
			}
		}
	}
	return -1, allocateFile
}

func (c *Coordinator) AllocateReduceJob(workerId int) (int, []string) {
	var allocateNumber int
	var reduceFileList []string
	if c.numReduce > 0 {
		for reduceJob := range c.reduceJobStatus {
			if c.numReduce == 0 {
				break
			}
			if c.reduceJobStatus[reduceJob][1] == JOBIDLE {
				//首先获取当前任务的写锁
				c.reduceJobLocks[reduceJob].Lock()
				defer c.reduceJobLocks[reduceJob].Unlock()
				//再次判断，避免死锁
				if c.reduceJobStatus[reduceJob][1] == JOBIDLE {
					//修改状态为 JOBBUSY处理中
					(*c.reduceJobStatus[reduceJob])[1] = JOBBUSY
					(*c.reduceJobStatus[reduceJob])[2] = workerId
					allocateNumber = (*c.reduceJobStatus[reduceJob])[0]
					reduceFileList = c.reduceInputs[allocateNumber]
					return allocateNumber, reduceFileList
				}
			}
		}
	}
	return -1, nil
}

func (c *Coordinator) AllocateJobRpc(args *AskForJobArgs, reply *AskForJobReply) error {
	//首先判断是否存在map任务
	reply.IsOver = false
	if c.numMap != 0 {
		allocateNumber, allocateFile := c.AllocateMapJob(args.WorkerId)
		if allocateNumber != -1 {
			reply.JobType = JOBTYPEMAP
			reply.FileList = []string{allocateFile}
			reply.JobNumber = allocateNumber
			log.Printf("allocate map job %d:%v to %d", allocateNumber, allocateFile, args.WorkerId)
			//启动线程，10秒种后若结果没有返回,将任务重置为未分配状态
			go func() {
				time.Sleep(10 * time.Second)
				if c.mapJobStatus[allocateFile][1] != JOBCOMPLETED {
					c.mapJobLocks[allocateFile].Lock()
					defer c.mapJobLocks[allocateFile].Unlock()
					if c.mapJobStatus[allocateFile][1] != JOBCOMPLETED {
						c.mapJobStatus[allocateFile][1] = JOBIDLE
					}
				}
			}()
		}
		return nil
	}
	//不存在map任务，则分配reduce任务
	if c.numReduce != 0 {
		allocateNumber, allocateFileList := c.AllocateReduceJob(args.WorkerId)
		if allocateNumber != -1 {
			reply.JobType = JOBTYPEREDUCE
			reply.FileList = allocateFileList
			reply.JobNumber = allocateNumber
			log.Printf("allocate reduce job %d to %d", allocateNumber, args.WorkerId)
			//启动线程，10秒种后若结果没有返回,将任务重置为未分配状态
			go func() {
				time.Sleep(10 * time.Second)
				if c.reduceJobStatus[allocateNumber][1] != JOBCOMPLETED {
					c.reduceJobLocks[allocateNumber].Lock()
					defer c.reduceJobLocks[allocateNumber].Unlock()
					if c.reduceJobStatus[allocateNumber][1] != JOBCOMPLETED {
						c.reduceJobStatus[allocateNumber][1] = JOBIDLE
					}
				}
			}()
			return nil
		}
	}
	log.Printf("no job for %d to do", args.WorkerId)
	//不存在可分配任务，设置任务为空
	reply.JobType = JOBTYPENONE
	return nil
}

func (c *Coordinator) AccomplishMapJobRpc(args *MapJobAccomplishCallBack, relpy *JobAccomplishReply) error {
	//若返回任务完成 或者 完成任务的workerid不等于记录中的workerId，直接返回false
	relpy.Success = false
	c.mapJobLocks[args.MapJobString].Lock()
	defer c.mapJobLocks[args.MapJobString].Unlock()
	if c.mapJobStatus[args.MapJobString][1] != JOBBUSY || c.mapJobStatus[args.MapJobString][2] != args.WokerId {
		log.Printf("worker %d complete job %d:%v, but discard", args.WokerId, args.MapJobNumber, args.MapJobString)
		return nil
	}
	//修改状态
	c.reduceInputsLock.Lock()
	defer c.reduceInputsLock.Unlock()
	//将map结果存储在map中
	for index, file := range args.FileList {
		// 文件名添加到对应切片中
		c.reduceInputs[index] = append(c.reduceInputs[index], file)
	}
	relpy.Success = true
	c.mapJobStatus[args.MapJobString][1] = JOBCOMPLETED
	//将待完成mapjob数量减一
	c.mapJobNumberLock.Lock()
	defer c.mapJobNumberLock.Unlock()
	c.numMap -= 1
	log.Printf("worker %d complete job %d:%v,left job %d", args.WokerId, args.MapJobNumber, args.MapJobString, c.numMap)
	return nil
}

func (c *Coordinator) AccomplishReduceJobRpc(args *ReduceJobAccomplishCallBack, relpy *JobAccomplishReply) error {
	//若返回任务完成 或者 完成任务的workerid不等于记录中的workerId，直接返回false
	relpy.Success = false
	c.reduceJobLocks[args.ReduceJobId].Lock()
	defer c.reduceJobLocks[args.ReduceJobId].Unlock()
	if c.reduceJobStatus[args.ReduceJobId][1] != JOBBUSY || c.reduceJobStatus[args.ReduceJobId][2] != args.WokerId {
		log.Printf("worker %d complete reduce job %d:%v, but discard", args.WokerId, args.ReduceJobId, args.ReduceFile)
		return nil
	}
	//修改状态
	relpy.Success = true
	c.reduceJobStatus[args.ReduceJobId][1] = JOBCOMPLETED
	//将待完成reducejob数量减一
	c.reduceJobNumberLock.Lock()
	defer c.reduceJobNumberLock.Unlock()
	c.numReduce -= 1
	log.Printf("worker %d complete reduce job %d:%v,left job %d", args.WokerId, args.ReduceJobId, args.ReduceFile, c.numReduce)
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	return c.numMap == 0 && c.numReduce == 0
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.

	//初始化Corrdinator
	c := Coordinator{}
	//1.workerId初始化
	c.numWorkers = 0
	c.totalReduceNumber = nReduce
	c.totalMapNumber = len(files)
	//2. 记录map和reduce状态相关map和锁
	c.numMap = len(files)
	c.numReduce = nReduce
	c.mapJobStatus = make(map[string]*[3]int)
	c.mapJobLocks = make(map[string]*sync.RWMutex)
	c.reduceJobStatus = make(map[int]*[3]int)
	c.reduceJobLocks = make(map[int]*sync.RWMutex)
	//记录所有的map任务
	for index, file_name := range files {
		c.mapJobStatus[file_name] = &[3]int{index, 0, 0}
		c.mapJobLocks[file_name] = &sync.RWMutex{}
	}
	//3. 记录raduce任务和所有reducer任务输入
	c.reduceInputs = make([][]string, nReduce)
	for i := 0; i < c.numReduce; i++ {
		c.reduceJobStatus[i] = &[3]int{i, 0, 0}
		c.reduceInputs[i] = make([]string, 0)
		c.reduceJobLocks[i] = &sync.RWMutex{}
	}
	//启动服务器，等待连接
	c.server()

	return &c
}
