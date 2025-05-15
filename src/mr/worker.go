package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

var pid int

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
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
	pid = os.Getpid()
loop:
	for {
		r, e := CallTask()
		if e != nil {
			break
		}
		switch r.TaskType {
		case "map":
			if taskMap(r.TaskId, r.Inputfiles, r.Outputfiles, mapf) == nil && CallFinish(r.TaskId) != nil {
				break loop
			}
		case "reduce":
			if taskReduce(r.TaskId, r.Inputfiles, r.Outputfiles[0], reducef) == nil && CallFinish(r.TaskId) != nil {
				break loop
			}
		case "wait":
			time.Sleep(1 * time.Second)
		case "done": // 所有任务完成，worker退出
			break loop
		default:
			fmt.Printf("unknown task type %s\n", r.TaskType)
		}

	}

}

func fileExists(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil || os.IsExist(err)
}

func taskReduce(TaskId int, Inputfiles []string, outputfile string, reducef func(string, []string) string) error {
	intermediate := []KeyValue{}
	for _, filename := range Inputfiles {
		file, err := os.Open(filename)
		if err != nil { // 有些哈希值的中间文件可能没有
			continue
		}
		decoder := json.NewDecoder(file)
		var kva []KeyValue
		err = decoder.Decode(&kva)
		file.Close()
		if err != nil {
			log.Fatalf("cannot decode json file %v", filename)
		}
		file.Close()
		intermediate = append(intermediate, kva...)
	}
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	ofile, _ := os.Create(outputfile)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to outputfile.
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
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	return nil
}

func taskMap(TaskId int, Inputfiles []string, Outputfiles []string, mapf func(string, string) []KeyValue) error {
	nReduce := len(Outputfiles)
	// 读取输入文件，调用mapf函数形成kv对序列
	intermediate := []KeyValue{}
	for _, filename := range Inputfiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}
	// 将kv对序列写入中间文件
	// 按照key的哈希值h排序，然后具有相同哈希值的kv对放入同一个中间文件mr-{TaskId}-{h}
	sort.Slice(intermediate, func(i, j int) bool {
		hi, hj := ihash(intermediate[i].Key)%nReduce, ihash(intermediate[j].Key)%nReduce
		return hi < hj
	})

	for i, j, h := 0, 0, 0; i < len(intermediate); i = j {
		// [i,j) 是同一个哈希值的kv对，需要放入同一个中间文件
		j, h = i+1, ihash(intermediate[i].Key)%nReduce
		for j < len(intermediate) && h == ihash(intermediate[j].Key)%nReduce {
			j++
		}
		// 存在中间文件mr-{TaskId}-{h}，则跳过,其他worker也可能在创建此中间文件
		if fileExists(Outputfiles[h]) {
			continue
		}
		// 在当前目录创建临时文件
		tempFile, err := os.CreateTemp(".", "temp_*.json")
		if err != nil {
			fmt.Println("创建临时文件失败:", err)
			return err
		}

		// 写入 JSON 数据（格式化）
		encoder := json.NewEncoder(tempFile)
		encoder.SetIndent("", "  ") // 美化格式
		err = encoder.Encode(intermediate[i:j])

		tempFile.Close()

		if err != nil {
			fmt.Println("写入 JSON 失败:", err)
			return err
		}

		// 重命名为最终文件
		finalPath := filepath.Join(".", Outputfiles[h])
		err = os.Rename(tempFile.Name(), finalPath)
		if err != nil {
			fmt.Println("重命名失败:", err)
			return err
		}
	}

	return nil
}

func CallTask() (WcReply, error) {
	// declare an argument structure.
	args := WcArgs{}

	// fill in the argument(s).
	args.Status = "ask"
	args.TaskId = -1

	// declare a reply structure.
	reply := WcReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Wc" tells the
	// receiving server that we'd like to call
	// the Wc() method of struct Coordinator.
	ok := call("Coordinator.Wc", &args, &reply)
	if ok {
		fmt.Printf("[CallTask %d] reply %+v\n", pid, reply)
		return reply, nil
	} else { // 可能是网络问题，尝试3次，失败则退出worker，也可能是corrdinator下线
		fmt.Printf("[CallTask %d] failed!\n", pid)
		for i := 1; i <= 3; i++ {
			time.Sleep(1 * time.Second)
			ok := call("Coordinator.Wc", &args, &reply)
			if ok {
				fmt.Printf("[CallTask %d] reply %+v\n", pid, reply)
				return reply, nil
			}
			fmt.Printf("[CallTask %d] failed! try %d/3\n", pid, i)
		}
		return reply, fmt.Errorf("call failed")
	}
}

func CallFinish(TaskId int) error {
	// declare an argument structure.
	args := WcArgs{}

	// fill in the argument(s).
	args.Status = "done"
	args.TaskId = TaskId

	// declare a reply structure.
	reply := WcReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Wc" tells the
	// receiving server that we'd like to call
	// the Wc() method of struct Coordinator.
	ok := call("Coordinator.Wc", &args, &reply)
	if ok {
		fmt.Printf("[CallFinish %d] taskid=%d\n", pid, TaskId)
		return nil
	} else {
		fmt.Printf("[CallFinish %d] failed!\n", pid)
		for i := 1; i <= 3; i++ {
			time.Sleep(1 * time.Second)
			ok := call("Coordinator.Wc", &args, &reply)
			if ok {
				fmt.Printf("[CallFinish %d] taskid=%d\n", pid, TaskId)
				return nil
			}
			fmt.Printf("[CallFinish %d] failed! try %d/3\n", pid, i)
		}
		return fmt.Errorf("call failed")
	}
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
