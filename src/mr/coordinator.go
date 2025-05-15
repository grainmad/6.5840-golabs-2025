package mr

import (
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
	nMap       int
	nReduce    int
	mapfiles   []string
	taskStatus []int // 0: not start, 1: start, 2: done
	taskBegin  []int64
	mu         sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// worker 请求任务或汇报完成任务
func (c *Coordinator) Wc(args *WcArgs, reply *WcReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.TaskId = -1
	if args.Status == "ask" {
		status := 2
		for i := 0; i < c.nMap; i++ {
			status = min(status, c.taskStatus[i])
			if c.taskStatus[i] == 0 && reply.TaskId == -1 {
				*reply = WcReply{
					TaskType:    "map",
					TaskId:      i,
					Inputfiles:  []string{c.mapfiles[i]},
					Outputfiles: make([]string, c.nReduce),
				}
				for j := 0; j < c.nReduce; j++ {
					reply.Outputfiles[j] = "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(j)
				}
				c.taskBegin[i] = time.Now().Unix()
				c.taskStatus[i] = 1 // 任务开始
			}
		}
		if status == 1 {
			*reply = WcReply{
				TaskType:    "wait",
				TaskId:      -1,
				Inputfiles:  []string{},
				Outputfiles: []string{},
			}
		}
		if status < 2 {
			return nil
		}
		// reduce
		for i := c.nMap; i < c.nMap+c.nReduce; i++ {
			status = min(status, c.taskStatus[i])
			if c.taskStatus[i] == 0 && reply.TaskId == -1 {
				*reply = WcReply{
					TaskType:    "reduce",
					TaskId:      i,
					Inputfiles:  make([]string, c.nMap),
					Outputfiles: []string{"mr-out-" + strconv.Itoa(i-c.nMap)},
				}
				for j := 0; j < c.nMap; j++ {
					reply.Inputfiles[j] = "mr-" + strconv.Itoa(j) + "-" + strconv.Itoa(i-c.nMap)
				}
				c.taskBegin[i] = time.Now().Unix()
				c.taskStatus[i] = 1 // 任务开始
			}
		}
		if status == 1 {
			*reply = WcReply{
				TaskType:    "wait",
				TaskId:      -1,
				Inputfiles:  []string{},
				Outputfiles: []string{},
			}
		}
		if status == 2 {
			*reply = WcReply{
				TaskType:    "done",
				TaskId:      -1,
				Inputfiles:  []string{},
				Outputfiles: []string{},
			}
		}

	} else if args.Status == "done" {
		c.taskStatus[args.TaskId] = 2 // 任务完成
	}
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//

// 协调器判断所有任务是否完成，重新分配超时任务
func (c *Coordinator) Done() bool {
	ret := true
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < c.nMap+c.nReduce; i++ {
		if c.taskStatus[i] != 2 {
			ret = false
		}
		if c.taskStatus[i] == 1 && time.Now().Unix()-c.taskBegin[i] > 10 {
			c.taskStatus[i] = 0 // 任务超时，重新分配
		}
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nMap:       len(files),
		nReduce:    nReduce,
		mapfiles:   files,
		taskStatus: make([]int, len(files)+nReduce),
		taskBegin:  make([]int64, len(files)+nReduce),
	}

	c.server()
	return &c
}
