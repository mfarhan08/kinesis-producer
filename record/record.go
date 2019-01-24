package record

import (
	"github.com/lordfarhan40/kinesis-producer/utils"
)

type Record struct {
	//The payload of the event to be delivered
	Data []byte

	//Optional
	PartitionKey string
}

func New(data []byte) *Record {
	return &Record{
		Data:         utils.CloneByteSlice(data),
		PartitionKey: utils.GenerateUUID(),
	}
}

func (record *Record) WithPartitionKey(partitionKey string) *Record {
	return &Record{
		Data:         utils.CloneByteSlice(record.Data),
		PartitionKey: partitionKey,
	}
}
