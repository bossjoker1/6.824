package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"sync"
)
import "strconv"

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

type AskTask struct {
	WorkID int
}

// worker请求master后，获得的reply信息
type Task struct {
	// mr-X-Y
	// Nmap int // X
	// 如果是map任务，则返回要分片的文件名
	Filename string
	// 如果是reduce任务，返回中间文件名集合
	Fs     []string
	WorkID int
	// 定值
	NReduce int
	// job type
	JType int
	MX    sync.Mutex
}

// 中间文件请求
type InterArgs struct {
	WorkId int
	Fs     []string
}

type ReduceDoneArgs struct {
	WorkId int
}

type NoReply struct {
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
