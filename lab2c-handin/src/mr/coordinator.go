package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	Tasks         map[int]*Task
	Wg            sync.WaitGroup
	Lock          sync.Mutex
	ReduceNum     int
	ReduceTook    map[int]bool
	ReduceDone    map[int]bool
	MapHasDone    int
	ReduceHasDone int
}

type Task struct {
	FileName string
	MapID    int
	ReduceID int
	MapTook  bool
	MapDone  bool
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) MapTimeCount(MapID int) {
	time.Sleep(15 * time.Second)
	c.Lock.Lock()
	defer c.Lock.Unlock()

	if !c.Tasks[MapID].MapDone {
		fmt.Println("MapTimeOut:" + strconv.Itoa(MapID))
		c.Tasks[MapID].MapTook = false
	}

}

func (c *Coordinator) ReduceTimeCount(ReduceID int) {
	time.Sleep(10 * time.Second)
	c.Lock.Lock()
	defer c.Lock.Unlock()

	if !c.ReduceDone[ReduceID] {
		fmt.Println("ReduceTimeOut:" + strconv.Itoa(ReduceID))
		c.ReduceTook[ReduceID] = false
	}
}

func (c *Coordinator) MapFinished(args *MapFinishedArgs, reply *MapFinishedReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	//如果任务已经超时，不再接受
	if !c.Tasks[args.MapID].MapTook {
		return nil
	}
	fmt.Println("MapFinished:" + strconv.Itoa(args.MapID))
	c.Tasks[args.MapID].MapDone = true
	c.MapHasDone++
	return nil
}
func (c *Coordinator) ReduceFinshed(args *ReduceFinishedArgs, reply *ReduceFinishedReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	//如果任务已经超时，不再接受
	if !c.ReduceTook[args.ReduceID] {
		return nil
	}
	fmt.Println("ReduceFinshed:" + strconv.Itoa(args.ReduceID) + strconv.Itoa(args.MapID))
	c.ReduceDone[args.ReduceID] = true
	c.ReduceHasDone++
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	for k, v := range c.Tasks {
		if !v.MapTook {
			reply.TaskRpl = *v
			reply.ReduceNum = c.ReduceNum
			c.Tasks[k].MapTook = true
			go c.MapTimeCount(k)
			break
		}
	}
	if c.MapHasDone == len(c.Tasks) {
		for i := 0; i < c.ReduceNum; i++ {
			if !c.ReduceTook[i] {
				reply.TaskRpl = Task{
					FileName: "",
					MapID:    -1,
					ReduceID: i,
					MapTook:  true,
				}
				reply.ReduceNum = c.ReduceNum
				c.ReduceTook[i] = true
				go c.ReduceTimeCount(i)
				break
			}
		}

	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	c.Lock.Lock()
	defer c.Lock.Unlock()
	// Your code here.
	return c.MapHasDone == len(c.Tasks) && c.ReduceHasDone == c.ReduceNum
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.ReduceNum = nReduce
	c.Tasks = make(map[int]*Task)
	c.ReduceTook = make(map[int]bool)
	c.ReduceDone = make(map[int]bool)
	// Your code here.
	for k, v := range files {
		t := Task{
			FileName: v,
			MapID:    k + 1,
			ReduceID: -1,
			MapTook:  false,
		}
		c.Tasks[t.MapID] = &t
	}
	c.server()
	return &c
}
