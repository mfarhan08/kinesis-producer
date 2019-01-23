package kinesis_producer

import (
	"errors"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/lordfarhan40/kinesis-producer/utils"
	"github.com/sirupsen/logrus"
)

const (
	MaxRecordSize        = 1 << 20
	MaxPutRequestSize    = MaxRecordSize * 5
	MaxRecordsPerRequest = 500
)

var (
	ErrRecordSizeExceeded        = errors.New("kinesis: record size exceeded")
	ErrRecordsPerRequestExceeded = errors.New("kinesis: max records per request exceeded")
)

type Producer struct {
	streamName string
	client     *kinesis.Kinesis
	log        *logrus.Logger
}

func (producer *Producer) SetLogger(logger *logrus.Logger) {
	producer.log = logger
}

func New(streamName string, awsSession *session.Session) *Producer {
	kinesis_client := kinesis.New(awsSession)
	return &Producer{
		streamName: streamName,
		client:     kinesis_client,
		log:        logrus.New(),
	}
}

func createPutRecordsRequestEntry(data []byte) *kinesis.PutRecordsRequestEntry {
	randomPartitionKey := utils.GenerateUUID()
	return &kinesis.PutRecordsRequestEntry{
		Data:         data,
		PartitionKey: &randomPartitionKey,
	}
}

func createPutRecordsInput(streamName string, data [][]byte) *kinesis.PutRecordsInput {
	putRecordsInput := kinesis.PutRecordsInput{
		StreamName: &streamName,
	}
	for _, record := range data {
		putRecordsInput.Records = append(
			putRecordsInput.Records,
			createPutRecordsRequestEntry(record),
		)
	}
	return &putRecordsInput
}

func (producer *Producer) PutRecords(data [][]byte) (*kinesis.PutRecordsOutput, error) {
	if len(data) > MaxRecordsPerRequest {
		return nil, ErrRecordsPerRequestExceeded
	}

	producer.log.Info("PutRecords: Going to write data to kinesis")

	resp, err := producer.client.PutRecords(createPutRecordsInput(producer.streamName, data))

	if err != nil {
		producer.log.Error(err)
	}
	return resp, err
}

func createPutRecordInput(streamName *string, data []byte) *kinesis.PutRecordInput {
	randomPartitionKey := utils.GenerateUUID()
	return &kinesis.PutRecordInput{
		PartitionKey: &randomPartitionKey,
		StreamName:   streamName,
		Data:         data,
	}
}

func (producer *Producer) PutRecord(data []byte) (*kinesis.PutRecordOutput, error) {
	if len(data) > MaxRecordSize {
		return nil, ErrRecordSizeExceeded
	}

	input := createPutRecordInput(&producer.streamName, data)

	producer.log.Info("PutRecord: Going to write data to kinesis")

	resp, err := producer.client.PutRecord(input)

	if err != nil {
		producer.log.Error(err)
	}

	return resp, err
}
