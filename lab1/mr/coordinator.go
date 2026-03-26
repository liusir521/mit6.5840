package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mu               sync.Mutex // 共用锁
	files            []string
	nReduce          int
	mapTasks         []Task // map任务列表
	reduceTasks      []Task // reduce任务列表
	isMapFinished    bool   // map任务是否完成
	isReduceFinished bool   // reduce任务是否完成
	nextTaskId       int
}

// 任务结构体
type Task struct {
	TaskID          int
	TaskType        int
	TaskStatus      int // 当前任务状态
	NReduce         int // map任务需要知道分配到多少个reduce任务
	MapTaskIndex    int
	ReduceTaskIndex int
	FileName        string
	StartTime       time.Time // 用于判断超时
}

// Your code here -- RPC handlers for the worker to call.

// 获取任务
func (c *Coordinator) GetTask(args *GetTaskReq, reply *GetTaskReply) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	// 判断是否所有任务均完成
	if c.isMapFinished && c.isReduceFinished {
		reply.TaskType = ExitTask
		return nil
	}

	// 判断是否有超时任务
	c.checkTimeoutTask()

	// 判断map任务是否都完成
	if !c.isMapFinished {
		for i, task := range c.mapTasks {
			// 如果存在map waiting任务，则将其改为running，并返回给worker执行
			if c.mapTasks[i].TaskStatus == Waiting {
				reply.TaskID = task.TaskID
				reply.TaskType = task.TaskType
				reply.FileName = task.FileName
				reply.NReduce = task.NReduce
				reply.ReduceID = task.ReduceTaskIndex
				c.mapTasks[i].TaskStatus = Running
				c.mapTasks[i].StartTime = time.Now()
				return nil
			}
		}
		reply.TaskType = WaitingTask
		return nil
	}

	// 到这里说明map任务已经完成，判断reduce任务是否都完成
	if !c.isReduceFinished {
		for i, task := range c.reduceTasks {
			// 如果存在reduce waiting任务，则将其改为running，并返回给worker执行
			if c.reduceTasks[i].TaskStatus == Waiting {
				reply.TaskID = task.TaskID
				reply.TaskType = task.TaskType
				reply.AllMapNum = len(c.mapTasks)
				reply.ReduceID = task.ReduceTaskIndex
				c.reduceTasks[i].TaskStatus = Running
				c.reduceTasks[i].StartTime = time.Now()
				return nil
			}
		}

	}

	reply.TaskType = WaitingTask
	return nil
}

// 查看是否存在任务超时
func (c *Coordinator) checkTimeoutTask() {
	// 超时时间 10s
	timeout := 10 * time.Second

	// 如果 map 任务已经完成，则返回
	// if c.isMapFinished {
	// 	return
	// }
	// 这里不可以直接return，会导致下面的reduce任务无法判断

	now := time.Now()

	if !c.isMapFinished {
		for i := 0; i < len(c.mapTasks); i++ {
			task := c.mapTasks[i]
			// 判断正在执行的任务是否存在超时的
			if task.TaskStatus == Running && now.Sub(task.StartTime) > timeout {
				// 如果存在，将状态改为 waiting，等待其他 worker 来执行
				c.mapTasks[i].TaskStatus = Waiting
				// 这里不需要修改任务的time，后面检测到waiting时，会重新标记时间
				// return 不 return，继续判断下一个任务，否则每次只标记到了一个任务就返回了
			}
		}
	}

	// 如果 reduce 任务已经完成，则返回
	// 这里可以直接return，代表全部任务执行完毕
	if c.isReduceFinished {
		return
	}
	for i := 0; i < len(c.reduceTasks); i++ {
		task := c.reduceTasks[i]
		// 存在超时任务
		if task.TaskStatus == Running && now.Sub(task.StartTime) > timeout {
			// 将状态改为 waiting，等待其他 worker 来执行
			c.reduceTasks[i].TaskStatus = Waiting
			// 这里不需要修改任务的time，后面检测到waiting时，会重新标记时间
			// return
		}
	}
}

// 完成任务
func (c *Coordinator) CompleteTask(args *CompleteTaskReq, reply *CompleteTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 完成map任务
	switch args.TaskType {
	case MapTask:
		// map任务完成
		// 判断所有map任务是否完成

		allmapfinished := true
		for i, task := range c.mapTasks {
			if args.TaskID == task.TaskID {
				c.mapTasks[i].TaskStatus = Finished
			} else if task.TaskStatus != Finished {
				allmapfinished = false
			}
		}
		// 所有map任务完成
		c.isMapFinished = allmapfinished
	case ReduceTask:
		// 完成reduce任务
		// 检测reduce任务是否完成
		if c.isReduceFinished {
			return nil
		}

		allreducefinished := true
		for i, task := range c.reduceTasks {
			if args.TaskID == task.TaskID {
				c.reduceTasks[i].TaskStatus = Finished
			} else if task.TaskStatus != Finished {
				allreducefinished = false
			}
		}
		c.isReduceFinished = allreducefinished
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	if c.isReduceFinished {
		return true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	c := Coordinator{
		files:            files,
		nReduce:          nReduce,
		mapTasks:         make([]Task, len(files)),
		reduceTasks:      make([]Task, nReduce),
		nextTaskId:       0,
		isMapFinished:    false,
		isReduceFinished: false,
	}

	// map任务初始化
	for i, file := range files {
		c.mapTasks[i] = Task{
			TaskID:       i,
			TaskType:     MapTask,
			TaskStatus:   Waiting,
			NReduce:      nReduce,
			MapTaskIndex: i,
			FileName:     file,
			StartTime:    time.Now(),
		}
		c.nextTaskId++
	}

	// reduce任务初始化
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{
			TaskID:          i,
			TaskType:        ReduceTask,
			TaskStatus:      Waiting,
			ReduceTaskIndex: i,
		}
		c.nextTaskId++
	}

	c.server()
	return &c
}
