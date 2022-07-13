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
	"strconv"
	"time"
)

var (
	cnt int64 = 0
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	filename := ""
	fileId, nReduce := -1, 0
	mapFinished := false

	for !mapFinished {
		filename, fileId, nReduce, mapFinished = CallAskMap(fileId)
		// 已经分配完或者map tasks已经结束
		if fileId == -1 {
			time.Sleep(time.Second)
			continue
		}

		content, err := ioutil.ReadFile(filename)
		if err != nil {
			panic("readfile " + filename + "failed!\n")
		}

		kva := mapf(filename, string(content))
		var file *os.File
		jsonMap := map[string]*json.Encoder{}
		for _, v := range kva {
			hash := ihash(v.Key) % nReduce
			InterFileName := fmt.Sprintf("mr-%d-%d", fileId, hash)
			enc, ok := jsonMap[InterFileName]
			if !ok {
				file, err = os.OpenFile(InterFileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
				if err != nil {
					panic("cannot create file " + InterFileName + "\n")
				}
				enc = json.NewEncoder(file)
				jsonMap[InterFileName] = enc
			}
			err := enc.Encode(&v)
			if err != nil {
				panic("json encode failed!\n")
			}
		}
		file.Close()
	}

	reduceId := -1
	reduced := false
	fileSize := 0

	for !reduced {
		reduceId, reduced, fileSize = CallAskReduce(reduceId)
		// fmt.Println("reduce : ", reduceId)
		if reduceId == -1 {
			time.Sleep(time.Second)
			continue
		}
		intermediate := getKVSlice(reduceId, fileSize)

		if len(intermediate) == 0 {
			continue
		}
		sort.Sort(ByKey(intermediate))

		filename := "mr-out-" + strconv.Itoa(reduceId)
		file, err := ioutil.TempFile("", filename+"-")
		if err != nil {
			panic("create temp file " + file.Name() + "failed : " + err.Error() + "\n")
		}

		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}

			var output string
			output = reducef(intermediate[i].Key, values)
			fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)
			i = j
		}

		file.Close()
		err = os.Rename(file.Name(), filename)
		if err != nil {
			panic("rename " + file.Name() + " to " + filename + " failed!\n")
		}

		time.Sleep(time.Second)
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func CallAskMap(fileId int) (string, int, int, bool) {
	reply := MapReply{}
	ok := call("Coordinator.AssignMap", &AskMapTaskArgs{
		FileId: fileId,
	}, &reply)
	if ok {
		return reply.Filename, reply.FileId, reply.NReduce, reply.MapFinish
	} else {
		panic("ask map task failed!\n")
	}
}

func CallAskReduce(reduceId int) (int, bool, int) {
	reply := ReduceReply{}
	ok := call("Coordinator.AssignReduce", &AskReduceArgs{
		ReduceId: reduceId,
	}, &reply)
	if ok {
		return reply.ReduceId, reply.Reduced, reply.FileSize
	} else {
		panic("ask reduce task failed!\n")
	}
}

func getKVSlice(reduceId int, fileSize int) (kva []KeyValue) {
	// fmt.Println("get kv slice")
	for i := 0; i < fileSize; i++ {
		filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reduceId)
		f, err := os.Open(filename)
		if err != nil {
			continue
		}

		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	return
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

	// fmt.Println(err)
	return false
}
