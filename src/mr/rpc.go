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

type TaskType int

const (
	Map    TaskType = 0
	Reduce          = 1
	Sleep           = 2
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type RegisterWorkerArgs struct {
}

type RegisterWorkerResponse struct {
	WorkerId int
}

// Workers need to get tasks
type AssignTaskArgs struct {
	WorkerId int
}

type AssignTaskResponse struct {
	Filename      string
	ReduceContent []string
	Type          TaskType
	TaskId        int
}

// Once a Map job is done, we need to do a "shuffle". Ultimately the results of each map operation are stored locally on the workers.
// So when one worker gets a reduce job, it must search through its intermediate file and send the requested data to the other worker.
// For the purposes of this lab, I'm just going to assume it's ok that we can read from the same directory

// Contains the output of a worker's Map job
type MapDoneArgs struct {
	TaskId   int
	WorkerId int
}

// Coordinator's response to the worker's Map results
type MapDoneResponse struct {
}

// Contains the output of a worker's Reduce job
type ReduceDoneArgs struct {
	WorkerId int
}

// Coordinator's response to the worker's Reduce results
type ReduceDoneResponse struct {
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
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
