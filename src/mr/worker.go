package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

func getIntermediateFilename(mapN int, reduceN int) string {
	return fmt.Sprintf("./mr-%d-%d", mapN, reduceN)
}

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	res := register()

	run := true
	for run {
		run = execTask(mapf, reducef, getTask(res.Id))
	}
}

func execTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, task *GetTaskReturn) bool {

	switch task.TaskType {
	case "map":
		lines, err := readFile(task.Filepath)
		if err != nil {
			// for now lets shut down if we where unable to open file.
			// worker should reschedule this task
			return false
		}
		mResult := mapf(task.Filepath, lines)
		buckets := bucketKeys(mResult, task.NReduce)
		err = writeIntermediate(buckets, task.NMap)
		if err != nil {

			return false
		}
		return true
	case "reduce":
		// TODO: implement correct naming schema
		oname := "mr-out-" + task.Filepath
		ofile, _ := os.Create(oname)

		keys, err := readIntermediate(task.Filepath, task.NMap)
		if err != nil {
			return false
		}

		sort.Sort(ByKey(keys))

		fmt.Println("key len", len(keys))

		for i := 0; i < len(keys); {
			j := i + 1
			for j < len(keys) && keys[i].Key == keys[j].Key {
				j++
			}

			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, keys[k].Value)
			}

			out := reducef(keys[i].Key, values)
			fmt.Fprintf(ofile, "%v %v\n", keys[i].Key, out)

			i = j
		}
		return true
	case "wait":
		time.Sleep(time.Second * 1)
		return true
	case "exit":
	default:
		return false
	}

	return false
}

func readFile(filepath string) (string, error) {
	f, err := os.Open(filepath)
	if err != nil {
		fmt.Printf("Error opening task file: %v\n", err)
		return "", err
	}
	defer f.Close()

	content, err := io.ReadAll(f)
	if err != nil {
		fmt.Printf("Error reading task file: %v", err)
		return "", err
	}

	return string(content), nil
}

func bucketKeys(keys []KeyValue, nReduce int) map[int][]KeyValue {
	buckets := make(map[int][]KeyValue)
	for _, v := range keys {
		n := ihash(v.Key) % nReduce
		bucket, ok := buckets[n]
		if ok {
			buckets[n] = append(bucket, v)
		} else {
			arr := make([]KeyValue, 0)
			arr = append(arr, v)
			buckets[n] = arr
		}
	}

	return buckets
}

func writeIntermediate(buckets map[int][]KeyValue, id int) error {
	for bucket, values := range buckets {
		f, err := ioutil.TempFile(os.TempDir(), "mapreduce_intermediate")
		if err != nil {
			fmt.Printf("Error creating tmp file for intermediate output %v\n", err)
			return err
		}

		encoder := json.NewEncoder(f)
		for _, v := range values {
			encoder.Encode(v)
		}

		err = os.Rename(f.Name(), getIntermediateFilename(id, bucket))
		if err != nil {
			fmt.Printf("error renaming a file: %v", err)
			return err
		}
		err = f.Close()
		if err != nil {
			fmt.Printf("error closing renamed file: %v", err)
			return err
		}
	}

	return nil
}

func readIntermediate(filepath string, mapN int) ([]KeyValue, error) {
	reduce, _ := strconv.Atoi(filepath)

	results := []KeyValue{}
	for i := 0; i < mapN; i++ {
		filename := getIntermediateFilename(i, reduce)
		fmt.Println("Reading ", filename)
		lines, err := readFile(filename)
		if err != nil {
			fmt.Println("No intermediate file", filename)
			continue
		}

		decoder := json.NewDecoder(strings.NewReader(lines))
		var kv KeyValue
		for decoder.More() {
			decoder.Decode(&kv)
			results = append(results, kv)
		}
	}

	return results, nil
}

func getTask(workerId int) *GetTaskReturn {
	in := GetTask{}
	in.WorkerId = workerId
	out := GetTaskReturn{}
	// TODO: fix error handling when calling coordinator
	problem := call("Coordinator.GetTask", &in, &out)
	if !problem {
		panic(errors.New("got rpc error from coordinator"))
	}
	return &out
}

func register() *RegisterReturn {
	in := RegisterIn{}
	out := RegisterReturn{}
	problem := call("Coordinator.Register", &in, &out)
	if !problem {
		panic(errors.New("got rpc error from coordinator"))
	}
	return &out
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.Dial("unix", sockname)
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
