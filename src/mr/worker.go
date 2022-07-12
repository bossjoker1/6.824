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
	"sync"
	"sync/atomic"
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

// 任务结构
type Work struct {
	state    int //状态
	wtype    int // 0对应map, 1对应reduce
	id       int
	filename string
	mx       sync.Mutex
}

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

	for {

		go func() {
			// worker请求任务
			ask := AskTask{WorkID: int(cnt)}
			atomic.AddInt64(&cnt, 1)
			// fmt.Println("cnt: ", cnt)
			task := Task{}
			ok := call("Coordinator.AssignTask", &ask, &task)

			if ok {
				fmt.Println("get a task successfully!", task)

				// 开启心跳检测
			} else {
				fmt.Println("call failed!!!")
				log.Fatal("worker call coordinator failed.\n")
			}

			if task.JType == MAPTYPE {
				// 读取并处理文本文件
				filename := task.Filename
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				fmt.Println("file : ", file.Name())
				content, err := ioutil.ReadAll(file)
				// fmt.Println("content : ", content, err)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				err = file.Close()
				if err != nil {
					return
				}
				kva := mapf(filename, string(content))

				files := []string{}
				jsonMap := map[string]*json.Encoder{}
				// fmt.Println("kva: ", kva)
				for _, v := range kva {
					hash := ihash(v.Key) % task.NReduce
					InterFileName := fmt.Sprintf("mr-%d-%d", task.WorkID, hash)
					enc, ok := jsonMap[InterFileName]
					if !ok {
						file, err := os.OpenFile(InterFileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
						if err != nil {
							log.Fatalf("cannot create file %s", InterFileName)
						}
						enc = json.NewEncoder(file)
						jsonMap[InterFileName] = enc
					}
					err := enc.Encode(&v)
					if err != nil {
						fmt.Println("here!!!!!!")
						log.Fatal(err)
					}

					files = append(files, InterFileName)
				}
				// fmt.Println("one map task finished : ", task.WorkID, files)
				// map任务完成
				call("Coordinator.MapFinishedToReduce", &InterArgs{
					WorkId: task.WorkID,
					Fs:     files,
				}, &NoReply{})
			} else if task.JType == REDUCETYPE {
				// reduce task
				if len(task.Fs) > 0 {
					intermediate := getKVSlice(task.Fs)
					sort.Sort(ByKey(intermediate))

					_, err := os.Stat(fmt.Sprintf("mr-out-%d", task.WorkID))

					if os.IsNotExist(err) {
						file, err := ioutil.TempFile("", "mr-temp-")
						if err != nil {
							log.Fatalf("cannot create temp file")
						}

						//
						// call Reduce on each distinct key in intermediate[],
						// and print the result to mr-out-0.
						//
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
							//if i == 0 {
							//	fmt.Println("output", output, intermediate[i].Key)
							//}

							if len(output) == 0 {
								err = file.Close()
								if err != nil {
									log.Fatalln("[error]:", err)
								}
								err := os.Remove(file.Name())
								if err != nil {
									return
								}
								log.Println("time out, worker crashed!")
								continue
							}

							// this is the correct format for each line of Reduce output.
							_, err := fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)
							if err != nil {
								log.Fatal("[error]:", err)
							}
							i = j
						}

						err = file.Close()
						if err != nil {
							log.Fatalln("[error]:", err)
						}
						err = os.Rename(file.Name(), fmt.Sprintf("mr-out-%d", task.WorkID))
						if err != nil {
							log.Fatalf("rename file failed  %v ", err)
						}
					}
				}

				log.Println("reduceTask Done")
				call("Coordinator.FinishedReduce", &ReduceDoneArgs{WorkId: task.WorkID}, &NoReply{})
			}
		}()

		time.Sleep(time.Second)
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func getKVSlice(fs []string) (kva []KeyValue) {
	for _, v := range fs {
		f, err := os.Open(v)
		if err != nil {
			log.Fatalf("open file %s failed!", v)
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

	fmt.Println(err)
	return false
}
