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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
)

func main() {
	session := session.New(aws.NewConfig())

	producer := kinesis_producer.New("test_stream", session)

    //Writing single record example
    input1 := []byte("Data Pack")
    producer.PutRecord(input1)
    
    //Writing multiple records example
	input2 := [][]byte{
		[]byte("Data Pack 1"),
		[]byte("Data Pack 2"),
	}

	producer.PutRecords(input2)
}
```