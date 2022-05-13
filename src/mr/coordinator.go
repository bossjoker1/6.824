package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type WorkInfo struct {
	WorkID int
	State  int
	WType  int
}

type Coordinator struct {
	// Your definitions here.
	files     []string // 要处理的文件名集合
	nReduce   int
	done      bool
	workerCnt int
	infos     []WorkInfo
	mx        sync.Mutex
	taskChans chan Task
}

func (c *Coordinator) MakeMapTask(idx int, filename string) Task {
	return Task{
		Nmap:     idx, // X
		NReduce:  c.nReduce,
		Filename: filename,
	}
}

// master Schedule
func (c *Coordinator) schedule() {
	for i, fname := range c.files {

		c.taskChans <- c.MakeMapTask(i, fname)

	}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *AskTask, reply *Task) error {
	id := args.WorkID

	if id >= len(c.files) {
		log.Fatal("idx excels the file array limit!")
		return fmt.Errorf("idx excels the file array limit!")
	}

	// 更新master记录的worker信息
	c.infos[id].WorkID = id
	c.infos[id].WType = MAPTYPE
	c.infos[id].State = RUNNING
	c.workerCnt++

	// 构造回复
	task := <-c.taskChans
	task.WorkID = id

	reply.NReduce = task.NReduce
	reply.Nmap = task.Nmap
	reply.WorkID = task.WorkID
	reply.Filename = task.Filename
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
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mx = sync.Mutex{}
	c.files = files
	if len(files) < nReduce {
		c.taskChans = make(chan Task, len(files))
	} else {
		c.taskChans = make(chan Task, nReduce)
	}

	c.nReduce = nReduce
	c.workerCnt = 0
	c.infos = make([]WorkInfo, MAXNUM)
	c.schedule()
	c.server()
	return &c
}
