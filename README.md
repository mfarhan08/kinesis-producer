# Kinesis-Producer
## Introduction
This is a very thin wrapper around kinesis go sdk that allows the user to put data into a kinesis stream without going into much details.
## Installation
Getting the package:
```go get github.com/mfarhan08/kinesis-producer```
## Example
``` go
package main

import (
	"fmt"

	kinesis_producer "github.com/lordfarhan40/kinesis-producer"
	"github.com/lordfarhan40/kinesis-producer/record"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
)

func main() {
	session := session.New(aws.NewConfig())
	producer := kinesis_producer.New("test_stream", session)

	//Writing single record example
	input1 := record.New([]byte("Hello"))
	output1, _ := producer.PutRecord(input1)

	fmt.Println("Result after PutRecord")
	fmt.Println(output1)

	//Print Records
	input2 := make([]*record.Record, 0)
	input2 = append(input2, record.New([]byte("Record 1")))
	input2 = append(input2, record.New([]byte("Record 2")))

	output2, _ := producer.PutRecords(input2)

	fmt.Println("Result after PutRecords")
	fmt.Println(output2)

	//Records with custom PartitionKey

	input3 := record.New([]byte("Custom Partition Key Paylod")).WithPartitionKey("Random Partition")

	output3, _ := producer.PutRecord(input3)

	fmt.Println(output3)
}

```