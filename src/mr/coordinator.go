package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"
)

type TaskStatus int

const (
	Idle      TaskStatus = 0
	Ongoing              = 1
	Completed            = 2
)

type Task struct {
	Type          TaskType
	Status        TaskStatus
	FileName      string
	ReduceContent []string
	TaskId        int
	StartTime     time.Time
}

type Coordinator struct {
	mu               sync.Mutex
	WorkerIds        []int
	MapTasks         []Task
	ReduceTasks      []Task
	MapComplete      bool
	ReduceComplete   bool
	NumReduce        int
	NumIntermediates int
	Keys             map[string]struct{}
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
	// fmt.Printf("New worker registered. Now there are %v Workers \n", len(c.WorkerIds))
	reply.WorkerId = workerId
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskResponse) error {
	// fmt.Printf("Task Requested from: %d\n", args.WorkerId)

	c.mu.Lock()

	if !c.MapComplete {
		AssignMap(c, args, reply)
	} else if c.MapComplete && !c.ReduceComplete {
		AssignReduce(c, args, reply)
	} else if c.MapComplete && c.ReduceComplete {
		AssignTerminate(c, args, reply)
	} else {
		reply.Type = Sleep
	}

	c.mu.Unlock()

	return nil
}

func AssignMap(c *Coordinator, args *AssignTaskArgs, reply *AssignTaskResponse) {
	i := -1
	for j, mapTask := range c.MapTasks {
		if mapTask.Status == Idle {
			i = j
			break
		}
	}
	if i == -1 {
		// fmt.Printf("Map tasks all assigned \n")
		reply.Type = Sleep
	} else {
		// fmt.Printf("Assigning map task: %d (%s) to worker %d \n", i, c.MapTasks[i].FileName, args.WorkerId)
		c.MapTasks[i].Status = Ongoing
		c.MapTasks[i].StartTime = time.Now()
		reply.Filename = c.MapTasks[i].FileName
		reply.TaskId = i
		reply.Type = Map
	}
}

func AssignReduce(c *Coordinator, args *AssignTaskArgs, reply *AssignTaskResponse) {
	ConditionalCreateReduce(c)
	i := -1
	for j, reduceTask := range c.ReduceTasks {
		if reduceTask.Status == Idle {
			i = j
			break
		}
	}
	if i == -1 {
		// fmt.Printf("Reduce tasks all assigned \n")
		reply.Type = Sleep
	} else {
		// fmt.Printf("Assigning reduce task: %d to worker %d \n", i, args.WorkerId)
		c.ReduceTasks[i].Status = Ongoing
		c.ReduceTasks[i].StartTime = time.Now()
		reply.ReduceContent = c.ReduceTasks[i].ReduceContent
		reply.NumIntermediates = c.NumIntermediates
		reply.TaskId = i
		reply.Type = Reduce
	}
}

func AssignTerminate(c *Coordinator, args *AssignTaskArgs, reply *AssignTaskResponse) {
	reply.TaskId = Terminate
}

func ConditionalCreateReduce(c *Coordinator) {
	if len(c.ReduceTasks) == 0 {

		fmt.Printf("Make reduce\n")

		c.ReduceTasks = make([]Task, c.NumReduce)

		tasks := make([][]string, c.NumReduce)

		keys := make([]string, 0, len(c.Keys))

		for k := range c.Keys {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		i := 1
		for _, key := range keys {
			tasks[i] = append(tasks[i], key)
			i += 1
			if i == c.NumReduce {
				i = 0
			}
		}

		index := 0
		for index < c.NumReduce {
			fmt.Printf("Reduce task: %d, has %d keys\n", index, len(tasks[index]))
			newReduceTask := Task{
				Type:          1,
				ReduceContent: tasks[index],
				Status:        Idle,
			}
			c.ReduceTasks[index] = newReduceTask
			index += 1
		}
	}
}

func (c *Coordinator) MapDone(args *MapDoneArgs, reply *MapDoneResponse) error {
	fmt.Printf("Processing map job: %d from worker %d \n", args.TaskId, args.WorkerId)

	c.mu.Lock()
	c.MapTasks[args.TaskId].Status = Completed
	for _, key := range args.Keys {
		c.Keys[key] = struct{}{}
	}
	c.mu.Unlock()

	return nil
}

func (c *Coordinator) ReduceDone(args *ReduceDoneArgs, reply *ReduceDoneResponse) error {
	fmt.Printf("Processing reduce job: %d from worker %d \n", args.TaskId, args.WorkerId)
	c.mu.Lock()
	c.ReduceTasks[args.TaskId].Status = Completed
	c.mu.Unlock()
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
	// fmt.Printf("Checking Done\n")
	c.mu.Lock()

	if !c.MapComplete {
		i := 0
		for i < len(c.MapTasks) {
			if c.MapTasks[i].Status != Completed {
				elapsed := time.Since(c.MapTasks[i].StartTime)
				if elapsed.Seconds() > 15 {
					c.MapTasks[i].Status = Idle
				}
				break
			}
			i += 1
		}
		if i == len(c.MapTasks) {
			fmt.Printf("Map stage complete\n")
			c.MapComplete = true
		}
	}

	if !c.ReduceComplete {
		j := 0
		for j < len(c.ReduceTasks) {
			if c.ReduceTasks[j].Status != Completed {
				elapsed := time.Since(c.ReduceTasks[j].StartTime)
				if elapsed.Seconds() > 15 {
					c.ReduceTasks[j].Status = Idle
				}
				break
			}
			j += 1
		}
		if j > 0 && j == len(c.ReduceTasks) {
			fmt.Printf("Reduce stage complete\n")
			c.ReduceComplete = true
		}
	}

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
	c.NumIntermediates = len(files)
	c.NumReduce = nReduce
	c.MapComplete = false
	c.ReduceComplete = false

	fmt.Printf("Reduce jobs: %d\n", nReduce)
	c.Keys = make(map[string]struct{})

	// fmt.Printf("Created MapTasks: %v\n", mapTasks)

	c.server()
	return &c
}
