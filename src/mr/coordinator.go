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

var (
	lock       = sync.Mutex{}
	reduceChan = make(chan ReduceTask, 10)
)

type WorkInfo struct {
	WorkID int
	State  int
	WType  int
}

type ReduceTask struct {
	WorkId int
	Fs     []string
}

type Coordinator struct {
	// Your definitions here.
	files       []string // 要处理的文件名集合
	nReduce     int
	done        bool
	workerCnt   int
	mapInfos    []WorkInfo
	reduceInfos []WorkInfo
	mx          sync.Mutex
	hmx         sync.Mutex
	taskChans   chan Task
	// 维护中间文件
	interFiles map[int][]string
	mapDone    int
	reduceDone int
}

func (c *Coordinator) MakeMapTask(filename string) Task {
	return Task{
		WorkID:   -1,
		NReduce:  c.nReduce,
		Filename: filename,
		JType:    MAPTYPE,
	}
}

// master Schedule
func (c *Coordinator) InitSchedule() {
	for _, fname := range c.files {

		c.taskChans <- c.MakeMapTask(fname)

	}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *AskTask, reply *Task) error {
	id := args.WorkID

	if id >= len(c.files) {
		// fmt.Println(" ")
		//log.Fatal("idx excels the file array limit!")
		//return fmt.Errorf("idx excels the file array limit!")
	}

	c.mx.Lock()

	select {
	case task := <-c.taskChans:
		if task.WorkID != -1 {
			id = task.WorkID
		}
		// 更新master记录的worker信息
		c.mapInfos[id].State = RUNNING
		c.mapInfos[id].WorkID = id
		c.mapInfos[id].WType = MAPTYPE
		c.workerCnt++
		// 构造回复
		task.WorkID = id
		reply.NReduce = task.NReduce
		reply.WorkID = task.WorkID
		reply.Filename = task.Filename
		reply.JType = task.JType

	case task := <-reduceChan:
		c.reduceInfos[task.WorkId].State = RUNNING
		c.reduceInfos[task.WorkId].WorkID = task.WorkId
		c.reduceInfos[task.WorkId].WType = REDUCETYPE
		c.workerCnt++
		reply.WorkID = task.WorkId
		reply.Fs = task.Fs
		reply.JType = REDUCETYPE

	default:
		reply.JType = EMPTY
		fmt.Println("waiting for...")
	}
	c.mx.Unlock()
	return nil
}

func (c *Coordinator) MapFinishedToReduce(args *InterArgs, reply *NoReply) error {
	// fmt.Println("to generate reduce files", args)
	for _, v := range args.Fs {
		var mid, rid int
		fmt.Sscanf(v, "mr-%d-%d", &mid, &rid)
		// 是否有重复元素
		isRepeat := false
		c.mx.Lock()
		for _, f := range c.interFiles[rid] {
			if v == f {
				isRepeat = true
			}
		}
		if isRepeat {
			c.mx.Unlock()
			continue
		}

		c.interFiles[rid] = append(c.interFiles[rid], v)
		c.mx.Unlock()
	}

	c.mx.Lock()
	c.mapDone++
	c.mapInfos[args.WorkId].State = FINISHED
	// map工作已经完成
	if c.mapDone == len(c.files) {
		for k, v := range c.interFiles {
			reduceChan <- ReduceTask{
				WorkId: k,
				Fs:     v,
			}
		}
	}
	c.mx.Unlock()
	return nil
}

func (c *Coordinator) FinishedReduce(args *ReduceDoneArgs, reply *NoReply) error {
	c.mx.Lock()
	c.reduceInfos[args.WorkId].State = FINISHED
	c.reduceDone++
	c.mx.Unlock()
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
	// ret := false

	// Your code here.

	return len(c.files) == c.mapDone && c.reduceDone == c.nReduce
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
	c.hmx = sync.Mutex{}
	c.files = files
	if len(files) < nReduce {
		c.taskChans = make(chan Task, len(files))
	} else {
		c.taskChans = make(chan Task, nReduce)
	}
	c.interFiles = make(map[int][]string)
	c.nReduce = nReduce
	c.workerCnt = 0
	c.mapDone = 0
	c.reduceDone = 0
	c.mapInfos = make([]WorkInfo, MAXNUM)
	c.reduceInfos = make([]WorkInfo, nReduce)
	c.InitSchedule()
	c.server()
	return &c
}
