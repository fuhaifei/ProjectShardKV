package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

func init() {
	log.SetOutput(ioutil.Discard)
}

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type SortByKey []KeyValue

// for sorting by key.
func (a SortByKey) Len() int           { return len(a) }
func (a SortByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SortByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	ok, workerId, reduceNumber := ReigsiterWorker()
	if ok {
		for taskEndFlag := false; !taskEndFlag; {
			ok, workInfo := CallForJob(workerId)
			if ok {
				if workInfo.IsOver {
					log.Printf("No job to do, exit later\n")
					taskEndFlag = true
				} else {
					switch workInfo.JobType {
					case JOBTYPEMAP:
						log.Printf("recieve map job %v, start Processing\n", workInfo.FileList[0])
						ok, intermediate_file_names := doMapWork(workInfo.JobNumber, workInfo.FileList[0], reduceNumber, workerId, mapf)
						if ok {
							log.Printf("process map job %v, success\n", workInfo.FileList[0])
							//写回结果
							ok = CallForMapJobAccomplished(workInfo.FileList[0], workerId, intermediate_file_names)
						}
						//若完成任务失败 或 返回结果失败，将所有的map输出删除
						if !ok {
							log.Printf("process map job %v, failed\n", workInfo.FileList[0])
							RemoveAllFiles(intermediate_file_names)
						}
					case JOBTYPEREDUCE:
						log.Printf("recieve reduce job %v, start Processing\n", workInfo.JobNumber)
						//进行reduce操作
						ok, reduceFileName := doReduceWork(workInfo.JobNumber, workInfo.FileList, workerId, reducef)
						if ok {
							log.Printf("process reduce job %v, success\n", workInfo.FileList[0])
							//写回结果
							ok = CallForReduceJobAccomplished(workInfo.JobNumber, reduceFileName, workerId)
						}
						if !ok {
							log.Printf("process reduce job %v, failed\n", workInfo.FileList[0])
							os.RemoveAll(reduceFileName)
						}
					default:
						log.Printf("none job recieved, waiting to next call\n")
					}
				}
			} else {
				log.Printf("failed to request a task, retry 2 seconds later")
			}
			time.Sleep(2 * time.Second)
		}
	}
	log.Printf("worker %d exit\n", workerId)
}

//我的工具方法块
func doMapWork(mapId int, filename string, nReduce int, workerid int, mapf func(string, string) []KeyValue) (bool, []string) {
	//中间文件
	intermediates := make([][]KeyValue, nReduce)
	//初始化二维切片
	for i := 0; i < nReduce; i++ {
		intermediates[i] = make([]KeyValue, 0)
	}
	//读取文件并输入map函数
	file, err := os.Open(filename)
	if err == nil {
		content, err := ioutil.ReadAll(file)
		file.Close()
		if err == nil {
			kva := mapf(filename, string(content))
			for _, aimKV := range kva {
				allocateIndex := ihash(aimKV.Key) % nReduce
				intermediates[allocateIndex] = append(intermediates[allocateIndex], aimKV)
			}
		} else {
			log.Fatalf("cannot read %v：%v", filename, err)
		}
	} else {
		log.Fatalf("cannot open %v：%v", filename, err)
	}
	//写入外存文件
	result_file_names := make([]string, 0)
	if err == nil {
		//遍历所有的intermediates写入中间文件
		for reduceNum, intermediate := range intermediates {
			intermediateFileName := fmt.Sprintf("mr-%d-%d-%d", workerid, mapId, reduceNum)
			tmpFile, err := ioutil.TempFile("", intermediateFileName)
			//关闭函数
			defer os.Remove(tmpFile.Name())
			if err != nil {
				log.Fatal("Cannot create temporary file", err)
			} else {
				for index, aimKey := range intermediate {
					if index < len(intermediate)-1 {
						fmt.Fprintf(tmpFile, "%v %v\n", aimKey.Key, aimKey.Value)
					} else {
						//最后一行不加换行符
						fmt.Fprintf(tmpFile, "%v %v", aimKey.Key, aimKey.Value)
					}
				}
			}
			err = os.Rename(tmpFile.Name(), intermediateFileName)
			if err != nil {
				log.Fatal("move file failed", err)
				return false, result_file_names
			}
			result_file_names = append(result_file_names, intermediateFileName)
		}
	}
	return true, result_file_names
}

func doReduceWork(reduceId int, reduceFileList []string, workerid int, reducef func(string, []string) string) (bool, string) {
	outputFileName := fmt.Sprintf("mr-out-%d", reduceId)
	//1.读取文件中的所有key
	allKeyValues := make([]KeyValue, 0)
	for index, fileName := range reduceFileList {
		content, err := os.ReadFile(fileName)
		if err != nil {
			log.Fatalf("read number %d input file %v failed:%v", index, fileName, err)
			return false, outputFileName
		}
		for _, kvStr := range strings.Split(string(content), "\n") {
			kv := strings.Split(kvStr, " ")
			if len(kv) == 2 {
				allKeyValues = append(allKeyValues, KeyValue{kv[0], kv[1]})
			}
		}
	}
	sort.Sort(SortByKey(allKeyValues))
	ofile, err := os.Create(outputFileName)
	if err != nil {
		log.Fatalf("ceate reduce output file failed %v\n", err)
		return false, outputFileName
	}
	total_length := 0
	i := 0
	for i < len(allKeyValues) {
		j := i + 1
		for j < len(allKeyValues) && allKeyValues[j].Key == allKeyValues[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, allKeyValues[k].Value)
		}
		output := reducef(allKeyValues[i].Key, values)
		tempNumber, _ := strconv.Atoi(output)
		total_length = total_length + tempNumber
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", allKeyValues[i].Key, output)
		i = j
	}
	ofile.Close()
	return true, outputFileName
}

func RemoveAllFiles(file_names []string) {
	//删除所有文件
	for _, file_name := range file_names {
		//确保删除
		os.RemoveAll(file_name)
	}
	log.Println("删除文件完成")
}

//我的rpc调用方法块

//1. 注册worker,获得workernum
func ReigsiterWorker() (bool, int, int) {
	args := EmptyArgs{}
	reply := RegisterWorkerReply{}
	for i := 0; i < 3; i++ {
		ok := call("Coordinator.RegisterWorker", &args, &reply)
		if ok {
			log.Printf("worker:%d register success\n", reply.WorkerId)
			return true, reply.WorkerId, reply.ReduceNum
		}
		time.Sleep(3 * time.Second)
	}
	log.Printf("worker register failed")
	return false, -1, -1
}

//2. 请求任务
func CallForJob(workerId int) (bool, AskForJobReply) {
	//请求工作的job
	args := AskForJobArgs{}
	workInfo := AskForJobReply{}
	args.WorkerId = workerId
	ok := call("Coordinator.AllocateJobRpc", &args, &workInfo)
	return ok, workInfo
}

//3. 返回请求任务结果
func CallForMapJobAccomplished(jobname string, workerId int, intermadiate_files []string) bool {
	//请求工作的job
	args := MapJobAccomplishCallBack{}
	reply := JobAccomplishReply{}
	args.MapJobString = jobname
	args.WokerId = workerId
	args.FileList = intermadiate_files
	//重新尝试三次
	ok := true
	for i := 0; i < 3; i++ {
		ok = call("Coordinator.AccomplishMapJobRpc", &args, &reply)
		if ok {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if !ok {
		log.Printf("return map job result fiaild %v", jobname)
	} else if !reply.Success {
		log.Printf("corrdinator refuse to accept map job %v", jobname)
	}
	return ok && reply.Success
}

func CallForReduceJobAccomplished(jobnumber int, reduce_output_file string, workerId int) bool {
	//请求工作的job
	args := ReduceJobAccomplishCallBack{}
	reply := JobAccomplishReply{}
	args.WokerId = workerId
	args.ReduceJobId = jobnumber
	args.ReduceFile = reduce_output_file
	//重新尝试三次
	ok := true
	for i := 0; i < 3; i++ {
		ok = call("Coordinator.AccomplishReduceJobRpc", &args, &reply)
		if ok {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if !ok {
		log.Printf("return reduce job result fiaild %d %v", jobnumber, reduce_output_file)
	} else if !reply.Success {
		log.Printf("corrdinator refuse to accept reduce job %d %v", jobnumber, reduce_output_file)
	}
	return ok && reply.Success
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
