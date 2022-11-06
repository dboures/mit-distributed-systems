package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	registerRequest := RegisterWorkerArgs{}
	registerReply := RegisterWorkerResponse{}
	registered := call("Coordinator.RegisterWorker", &registerRequest, &registerReply)
	if registered {
		fmt.Printf("Worker Registered: %d\n", registerReply.WorkerId)
	} else {
		return
	}

	workerId := registerReply.WorkerId

	for {
		taskRequest := AssignTaskArgs{WorkerId: workerId}
		taskReply := AssignTaskResponse{}
		fmt.Printf("Worker %d requesting task\n", workerId)
		ok := call("Coordinator.AssignTask", &taskRequest, &taskReply)
		if ok {
			if taskReply.Type == Map {
				DoMap(taskReply, workerId, mapf)
			} else if taskReply.Type == Reduce {
				// DoReduce(taskReply, workerId, reducef)
				time.Sleep(3 * time.Second)
			} else {
				fmt.Printf("Worker %d sleeping\n", workerId)
				time.Sleep(3 * time.Second)
			}
		} else {
			fmt.Printf("AssignTask call failed!\n")
		}
	}
}

func DoMap(mapTask AssignTaskResponse, workerId int, mapf func(string, string) []KeyValue) {
	fmt.Printf("map task\n")

	// read file
	file, err := os.Open(mapTask.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", mapTask.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", mapTask.Filename)
	}
	file.Close()
	//map
	result := mapf(mapTask.Filename, string(content))
	fmt.Printf("%d\n", len(result))

	// Write to disk
	oname := fmt.Sprintf("map-out-%d", mapTask.TaskId)
	ofile, _ := os.Create(oname)

	keys := make([]string, len(result))

	i := 0
	for i < len(result) {
		keys[i] = result[i].Key
		fmt.Fprintf(ofile, "%v\n", result[i])
		i += 1
	}

	ofile.Close()

	mapResponseArgs := MapDoneArgs{
		WorkerId: workerId,
		TaskId:   mapTask.TaskId,
		Keys:     keys,
	}
	mapResponseReply := MapDoneResponse{}
	processReduce := call("Coordinator.MapDone", &mapResponseArgs, &mapResponseReply)
	if !processReduce {
		fmt.Printf("MapDone call failed!\n")
	}
}

func DoReduce(reduceTask AssignTaskResponse, workerId int, reducef func(string, []string) string) {
	fmt.Printf("reduce task\n")

	// read all files, create intermediate

	// do reduce

	// save output

	// // result := reducef(taskReply.Filename, taskReply.ReduceContent)
	// // actually need to send back??
	// reduceResponseArgs := ReduceDoneArgs{
	// 	WorkerId: workerId,
	// }
	// reducResponseReply := ReduceDoneResponse{}
	// processReduce := call("Coordinator.ProcessReduce", &reduceResponseArgs, &reducResponseReply)
	// if !processReduce {
	// 	fmt.Printf("ProcessReduce call failed!\n")
	// }
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
