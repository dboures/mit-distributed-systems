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

type TaskStatus int

const (
	Idle      TaskStatus = 0
	Ongoing              = 1
	Completed            = 2
)

type Task struct {
	Type     TaskType
	Status   TaskStatus
	FileName string
	TaskId   int
}

type Coordinator struct {
	mu             sync.Mutex
	WorkerIds      []int
	MapTasks       []Task
	ReduceTasks    []Task
	MapComplete    bool
	ReduceComplete bool
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerResponse) error {
	c.mu.Lock()
	workerId := len(c.WorkerIds)
	c.WorkerIds = append(c.WorkerIds, workerId)
	fmt.Printf("New worker registered. Now there are %v Workers \n", len(c.WorkerIds))
	reply.WorkerId = workerId
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskResponse) error {
	fmt.Printf("Task Requested from: %d\n", args.WorkerId)

	c.mu.Lock()

	// try to assign MAP
	if !c.MapComplete {
		i := -1
		for j, mapTask := range c.MapTasks {
			if mapTask.Status == Idle {
				i = j
				break
			}
		}

		if i == -1 {
			fmt.Printf("Map Stage complete \n")
			c.MapComplete = true
			reply.Type = Sleep
		} else {
			fmt.Printf("Assigning map task: %d to worker %d \n", i, args.WorkerId)
			c.MapTasks[i].Status = Ongoing
			reply.Filename = c.MapTasks[i].FileName
			reply.TaskId = i
		}
	} else if !c.ReduceComplete {
		reply.Type = Sleep
	} else {
		reply.Type = Sleep
	}

	c.mu.Unlock()

	return nil
}

func (c *Coordinator) ProcessMap(args *MapDoneArgs, reply *MapDoneResponse) error {
	fmt.Printf("Processing map job: %d from worker %d \n", args.TaskId, args.WorkerId)

	c.mu.Lock()
	c.MapTasks[args.TaskId].Status = Completed
	c.mu.Unlock()

	return nil
}

func (c *Coordinator) ProcessReduce(args *ReduceDoneArgs, reply *ReduceDoneResponse) error {
	// TODO
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
	c.mu.Lock()
	done := c.MapComplete && c.ReduceComplete
	c.mu.Unlock()
	return done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Assign mapTasks
	mapTasks := make([]Task, len(files))
	for index, file := range files {
		newMapTask := Task{
			Type:     0,
			FileName: file,
			Status:   Idle,
		}
		mapTasks[index] = newMapTask
	}
	c.MapTasks = mapTasks

	// fmt.Printf("Created MapTasks: %v\n", mapTasks)

	// Your code here.

	c.server()
	return &c
}

// Somewhere we need to put a checker for crashed workers
