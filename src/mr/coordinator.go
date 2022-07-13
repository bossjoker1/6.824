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
	files          []string // 要处理的文件名集合
	filesize       int
	allocMap       []bool
	mapTime        []int64
	finishedMap    []bool
	finishedMapCnt int

	nReduce           int
	allocReduce       []bool
	reduceTime        []int64
	finishedReduce    []bool
	finishedReduceCnt int

	finishedMutex sync.RWMutex
	mx            sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignMap(args *AskMapTaskArgs, reply *MapReply) error {
	// 第二次请求，正常情况下已完成
	if args.FileId != -1 {
		if !c.finishedMap[args.FileId] {
			c.finishedMap[args.FileId] = true
			c.finishedMutex.Lock()
			c.finishedMapCnt++
			c.finishedMutex.Unlock()
		}

		c.finishedMutex.RLock()
		if c.finishedMapCnt == c.filesize {
			c.finishedMutex.RUnlock()
			reply.MapFinish = true // map task结束
			reply.FileId = -1
			return nil
		}
		c.finishedMutex.RUnlock()
	}

	// 第一次请求，返回map的文件名
	// 或者由于crash原因造成任务失败(超过10s还未完成)
	c.mx.Lock()
	for i := 0; i < c.filesize; i++ {
		if !c.allocMap[i] || (!c.finishedMap[i] && time.Now().Unix()-c.mapTime[i] > 10) {
			c.allocMap[i] = true
			c.mapTime[i] = time.Now().Unix()
			c.mx.Unlock()

			reply.MapFinish = false
			reply.Filename = c.files[i]
			reply.NReduce = c.nReduce
			reply.FileId = i
			return nil
		}
	}

	c.mx.Unlock()
	// 都已经分配完
	c.finishedMutex.RLock()
	reply.MapFinish = c.finishedMapCnt == c.filesize
	c.finishedMutex.RUnlock()
	reply.FileId = -1

	return nil
}

func (c *Coordinator) AssignReduce(args *AskReduceArgs, reply *ReduceReply) error {
	if args.ReduceId != -1 {
		if !c.finishedReduce[args.ReduceId] {
			c.finishedReduce[args.ReduceId] = true
			c.finishedMutex.Lock()
			c.finishedReduceCnt++
			c.finishedMutex.Unlock()
		}
		c.finishedMutex.RLock()
		if c.finishedReduceCnt == c.nReduce {
			c.finishedMutex.RUnlock()
			reply.Reduced = true
			reply.ReduceId = -1
			return nil
		}
		c.finishedMutex.RUnlock()
	}

	c.mx.Lock()
	for i := 0; i < c.nReduce; i++ {
		if !c.allocReduce[i] || (!c.finishedReduce[i] && time.Now().Unix()-c.reduceTime[i] > 10) {
			c.allocReduce[i] = true
			c.reduceTime[i] = time.Now().Unix()
			c.mx.Unlock()
			reply.Reduced = false
			reply.ReduceId = i
			reply.FileSize = c.filesize
			return nil
		}
	}

	c.mx.Unlock()
	c.finishedMutex.RLock()
	reply.Reduced = c.finishedReduceCnt == c.nReduce
	c.finishedMutex.RUnlock()
	reply.ReduceId = -1
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
	c.finishedMutex.RLock()
	ret := c.finishedReduceCnt == c.nReduce
	c.finishedMutex.RUnlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	l := len(files)
	c := Coordinator{
		files:          files,
		nReduce:        nReduce,
		filesize:       l,
		allocMap:       make([]bool, l),
		mapTime:        make([]int64, l),
		finishedMap:    make([]bool, l),
		finishedMapCnt: 0,

		allocReduce:       make([]bool, nReduce),
		reduceTime:        make([]int64, nReduce),
		finishedReduce:    make([]bool, nReduce),
		finishedReduceCnt: 0,
	}

	// Your code here.

	c.server()
	return &c
}
