package mr

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

const waitTime = 5 * time.Second

type Task struct {
	id        int
	taskId    int
	startTime time.Time
}

func (t *Task) Expired() bool {
	return time.Now().After(t.startTime.Add(waitTime))
}

func (t *Task) Renew(id int) {
	t.id = id
	t.startTime = time.Now()
}

func NewTask(id, taskId int) *Task {
	return &Task{id, taskId, time.Now()}
}

type Coordinator struct {
	sync.Mutex
	files           []string
	filesIndx       int
	nReduce         int
	reduceStarted   bool
	finished        bool
	workers         map[int]*Task
	reducersCount   int
	workerIdCounter int
}

// Your code here -- RPC handlers for the worker to call.

// Returns first available task to worker querying for it
func (c *Coordinator) GetTask(in *GetTask, out *GetTaskReturn) error {
	fmt.Println("Worker asked for task")

	c.Lock()
	if !c.reduceStarted {
		// MAP
		if c.filesIndx >= len(c.files) {
			delete(c.workers, in.WorkerId)

			if len(c.workers) == 0 {
				c.reduceStarted = true
			} else if t := c.GetStuckTask(); t != nil {
				t.Renew(in.WorkerId)
				c.workers[t.id] = t
				out.NMap = t.taskId
				out.NReduce = c.nReduce
				c.Unlock()

				out.Filepath = c.files[out.NMap]
				out.TaskType = "map"
				return nil
			}

			c.Unlock()
			fmt.Println("Map: Instructed worker", in.WorkerId, "to wait. Map still in progress")

			out.TaskType = "wait"
		} else {
			out.NMap = c.filesIndx
			c.filesIndx++
			c.workers[in.WorkerId] = NewTask(in.WorkerId, out.NMap)
			c.Unlock()

			out.Filepath = c.files[out.NMap]
			out.TaskType = "map"
			out.NReduce = c.nReduce
			fmt.Println("Map: Giving file", out.Filepath, "to worker", in.WorkerId)
		}
	} else {
		// REDUCE
		if c.reducersCount >= c.nReduce {
			delete(c.workers, in.WorkerId)

			if len(c.workers) == 0 {
				c.finished = true
			} else if t := c.GetStuckTask(); t != nil {
				t.Renew(in.WorkerId)
				c.workers[t.id] = t
				out.NMap = len(c.files)
				out.Filepath = strconv.Itoa(t.taskId)
				c.Unlock()

				out.TaskType = "reduce"
				return nil
			}
			c.Unlock()

			out.TaskType = "exit"
			fmt.Println("Reduce: Instruced worker", in.WorkerId, "to exit")
		} else {
			out.Filepath = strconv.Itoa(c.reducersCount)
			c.workers[in.WorkerId] = NewTask(in.WorkerId, c.reducersCount)
			c.reducersCount++
			out.NMap = len(c.files)
			c.Unlock()

			out.TaskType = "reduce"
			fmt.Println("Reduce: Giving reduce task", out.Filepath, "to worker id:", in.WorkerId)
		}
	}

	return nil
}

func (c *Coordinator) GetStuckTask() *Task {
	for id, task := range c.workers {
		if task.Expired() {
			delete(c.workers, id)
			return task
		}
	}

	return nil
}

func (c *Coordinator) Register(in *RegisterIn, out *RegisterReturn) error {
	c.Lock()
	out.Id = c.workerIdCounter
	c.workerIdCounter++
	c.Unlock()

	fmt.Println("Register: Register new worker id:", out.Id)
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)

	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go rpc.Accept(l)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.Lock()
	defer c.Unlock()
	return c.finished
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.files = files
	c.nReduce = nReduce
	c.workers = make(map[int]*Task)

	c.server()
	return &c
}
