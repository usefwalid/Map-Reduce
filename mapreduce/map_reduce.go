package mapreduce

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"sort"
	"strings"
)

func runMapTask(
	jobName string, // The name of the whole mapreduce job
	mapTaskIndex int, // The index of the map task
	inputFile string, // The path to the input file assigned to this task
	nReduce int, // The number of reduce tasks that will be run
	mapFn func(file string, contents string) []KeyValue, // The user-defined map function
) {
	content, _ := os.ReadFile(inputFile)
	text := string(content)
	InputFileName := strings.Split(inputFile, "/")[len(strings.Split(inputFile, "/"))-1]
	Key_Pairs := mapFn(text, InputFileName)
	for curr := range Key_Pairs {
		index := hash32(fmt.Sprint(curr)) % uint32(nReduce)
		OutputFileName := getIntermediateName(jobName, mapTaskIndex, int(index))
		file, _ := os.OpenFile(OutputFileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		encoder := json.NewEncoder(file)
		err := encoder.Encode(curr)
		if err != nil {
			fmt.Println("error writing to file")
		}
	}
}

func hash32(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func runReduceTask(
	jobName string,
	reduceTaskIndex int,
	nMap int,
	reduceFn func(key string, values []string) string,
) {
	var OutputFile string = getReduceOutName(jobName, reduceTaskIndex)
	Output := make(map[string]string)
	Mapper := make(map[string][]string)
	for i := 0; i < reduceTaskIndex; i++ {
		Name := getIntermediateName(jobName, nMap, reduceTaskIndex)

		file, _ := os.Open(Name)
		defer file.Close()

		decoder := json.NewDecoder(file)
		var kv KeyValue
		err := decoder.Decode(&kv)
		for err != io.EOF {

			Mapper[kv.Key] = append(Mapper[kv.Key], kv.Value)

			err := decoder.Decode(&kv)
			if err == io.EOF {
				break
			}
		}
	}

	var keys []string
	for key := range Mapper {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		Output[key] = reduceFn(key, Mapper[key])
		file, _ := os.OpenFile(OutputFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		encoder := json.NewEncoder(file)
		err := encoder.Encode(Output[key])
		if err != nil {
			fmt.Println("error writing to file")
		}
	}

}
