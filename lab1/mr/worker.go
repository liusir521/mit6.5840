package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
// 请求任务并执行
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {

		workerid := os.Getpid()
		reply := getWorkerTask(workerid)

		switch reply.TaskType {
		case MapTask:
			startMapTask(workerid, reply, mapf)
		case ReduceTask:
			startReduceTask(workerid, reply, reducef)
		case WaitingTask:
			// 休眠0.5秒
			time.Sleep(500 * time.Millisecond)
			continue
		case ExitTask:
			return
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func startReduceTask(workerid int, reply GetTaskReply, reducef func(string, []string) string) {
	// 根据传入的reduceNum确定所负责的任务index
	// 然后去所有map产出的文件中找到对应index的文件进行reduce

	kva := []KeyValue{}

	for i := 0; i < reply.AllMapNum; i++ {
		curfilename := fmt.Sprintf("mr-%d-%d", i, reply.ReduceID)

		file, err := os.Open(curfilename)
		if err != nil {
			log.Fatalf("reduce cannot open %v", file)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	// 对结果进行排序
	sort.Sort(ByKey(kva))

	tempfile, err := os.CreateTemp("", "mr-out-tmpfile-*")
	if err != nil {
		log.Fatal("create tempfile err", err)
	}

	// 参考示例代码处理键值对
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempfile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	tempfile.Close()

	os.Rename(tempfile.Name(), fmt.Sprintf("mr-out-%d", reply.ReduceID))

	completeTask(reply.TaskID, reply.TaskType)
}

// Map任务
func startMapTask(workerid int, reply GetTaskReply, mapf func(string, string) []KeyValue) {
	// 首先读取文件kv，并根据kv的hash分配到不同的切片中
	// 然后使用临时文件的方式，将这些kv存入对应的临时文件，最后写入对应的reduce文件中

	// 读取文件内容参考示例文件
	filename := reply.FileName
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("map cannot open %v %v", filename, err)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	// intermediate = append(intermediate, kva...)
	// 示例文件给出的是单机版本，这里需要实现并发版本

	intermediate := make([][]KeyValue, reply.NReduce)

	for _, kv := range kva {
		// 计算hash并分配
		index := ihash(kv.Key) % reply.NReduce
		intermediate[index] = append(intermediate[index], kv)
	}

	// 临时文件方式存储
	for i := 0; i < reply.NReduce; i++ {
		tempfile, err := os.CreateTemp("", "mr-tmpfile")
		if err != nil {
			log.Fatal("create tempfile err", err)
		}

		// 使用推荐的json包处理kv
		encoder := json.NewEncoder(tempfile)
		for _, kv := range intermediate[i] {
			err := encoder.Encode(kv)
			if err != nil {
				log.Fatal("json write tempfile err", err)
			}
		}
		tempfile.Close()
		reducename := fmt.Sprintf("mr-%d-%d", reply.TaskID, i)
		os.Rename(tempfile.Name(), reducename)
	}
	// 汇报任务执行完成
	completeTask(reply.TaskID, MapTask)
}

// 通过rpc获取任务
func getWorkerTask(workerID int) GetTaskReply {
	// rpc获取任务
	req := GetTaskReq{
		WorkerID: workerID,
	}

	reply := GetTaskReply{}

	call("Coordinator.GetTask", &req, &reply)

	return reply
}

// 汇报任务完成
func completeTask(taskid, tasktype int) {
	req := CompleteTaskReq{
		TaskID:   taskid,
		TaskType: tasktype,
	}

	reply := CompleteTaskReply{}

	call("Coordinator.CompleteTask", &req, &reply)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
