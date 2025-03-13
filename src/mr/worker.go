package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

func ReadFile(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}

	file.Close()
	return string(content)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		task := CallGetTask() //尝试获取任务
		if task == nil {
			return
		}
		if task.TaskRpl.MapID != 0 {
			if task.TaskRpl.MapID == -1 {
				// do reduce
				pattern := "mr-*-" + strconv.Itoa(task.TaskRpl.ReduceID) + ".mid"
				files, _ := filepath.Glob(pattern)
				var intermediate []KeyValue
				for _, file := range files {
					content := ReadFile(file)
					lines := strings.Split(content, "\n")
					for _, line := range lines {
						if line != "" {
							fmt.Println(line)
							kv := strings.Split(line, "\t")
							intermediate = append(intermediate, KeyValue{kv[0], kv[1]})
						}
					}
				}
				sort.Sort(ByKey(intermediate))
				ofile, err := os.Create("mr-out-" + strconv.Itoa(task.TaskRpl.ReduceID))
				if err != nil {
					fmt.Printf("create file error %v\n", err)
					continue
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
					output := reducef(intermediate[i].Key, values)

					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

					i = j
				}
				ofile.Close()
				CallReduceFinished(ReduceFinishedArgs{task.TaskRpl.ReduceID, task.TaskRpl.MapID})

			} else {
				// do map
				fmt.Println("do map: " + strconv.Itoa(task.TaskRpl.MapID))
				intermediate := []KeyValue{}
				content := ReadFile(task.TaskRpl.FileName)
				kva := mapf(task.TaskRpl.FileName, string(content))
				intermediate = append(intermediate, kva...)
				sort.Sort(ByKey(intermediate))

				mp := make(map[int][]KeyValue)

				for _, kv := range intermediate {
					// fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
					reduceID := ihash(kv.Key) % task.ReduceNum
					mp[reduceID] = append(mp[reduceID], kv)
				}
				//Get Reduce ID
				for k, v := range mp {
					ofile, err := os.Create("mr-" + strconv.Itoa(task.TaskRpl.MapID) + "-" + strconv.Itoa(k) + ".mid")
					if err != nil {
						fmt.Printf("create file error %v\n", err)
						continue
					}
					for _, kv := range v {
						fmt.Fprintf(ofile, "%v\t%v\n", kv.Key, kv.Value)
					}
					ofile.Close()
				}
				CallMapFinished(MapFinishedArgs{task.TaskRpl.MapID})

			}

		}

	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func CallGetTask() *GetTaskReply {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		// fmt.Printf("reply %v\n", reply.TaskRpl)
	} else {
		return nil
	}
	return &reply
}

func CallMapFinished(callArgs MapFinishedArgs) {
	reply := MapFinishedReply{}
	ok := call("Coordinator.MapFinished", &callArgs, &reply)
	if ok {
		// fmt.Printf("reply %v\n", reply)
	}
}

func CallReduceFinished(callArgs ReduceFinishedArgs) {
	reply := ReduceFinishedReply{}
	ok := call("Coordinator.ReduceFinshed", &callArgs, &reply)
	if ok {
		// fmt.Printf("reply %v\n", reply)
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
