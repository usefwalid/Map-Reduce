package main

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
)

func hash32(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

type KeyValue struct {
	Key   string
	Value string
}

func main() {
	OutputFile := "hi.txt"
	var x KeyValue
	x.Key = "hey"
	x.Value = "1"

	file, _ := os.OpenFile(OutputFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	encoder := json.NewEncoder(file)
	err := encoder.Encode(x.Key)
	encoder.Encode(x.Key)
	encoder.Encode(x.Key)

	if err != nil {
		fmt.Println("error writing to file")
	}

}
