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

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// 任务类型枚举定义
const (
	MapTask = iota
	ReduceTask
	WaitingTask // 任务正在执行，此时需要task等待
	ExitTask    // map、reduce任务已经执行完毕，退出信号
)

// task任务状态枚举定义
const (
	Waiting = iota
	Running
	Finished
)

// 获取任务请求
type GetTaskReq struct {
	WorkerID int
}

// 获取任务响应
type GetTaskReply struct {
	TaskID     int
	TaskType   int // 当收到ExitTask时，结束标志
	FileName   string
	AllMapNum  int // 一共有多少map任务，用于reduce过程，其实相当于map处理文件的个数，一个map任务对应一个文件
	ReduceID   int // 当前reduce任务负责的分区编号
	NReduce    int
	TaskStatus int
}

// 完成任务请求
type CompleteTaskReq struct {
	TaskType int
	TaskID   int
}

// 完成任务响应
type CompleteTaskReply struct {
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
