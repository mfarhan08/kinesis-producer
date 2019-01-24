package kinesis_producer

import (
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/lordfarhan40/kinesis-producer/record"
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

func createPutRecordsRequestEntry(record record.Record) *kinesis.PutRecordsRequestEntry {
	return &kinesis.PutRecordsRequestEntry{
		Data:         utils.CloneByteSlice(record.Data),
		PartitionKey: aws.String(record.PartitionKey),
	}
}

func createPutRecordsInput(streamName string, records []record.Record) *kinesis.PutRecordsInput {
	putRecordsInput := kinesis.PutRecordsInput{
		StreamName: aws.String(streamName),
	}
	for _, record := range records {
		putRecordsInput.Records = append(
			putRecordsInput.Records,
			createPutRecordsRequestEntry(record),
		)
	}
	return &putRecordsInput
}

func (producer *Producer) PutRecords(streamName string, records []record.Record) (*kinesis.PutRecordsOutput, error) {
	if len(records) > MaxRecordsPerRequest {
		return nil, ErrRecordsPerRequestExceeded
	}
	resp, err := producer.client.PutRecords(createPutRecordsInput(producer.streamName, records))

	if err != nil {
		producer.log.Error(err)
	}
	return resp, err
}

func createPutRecordInput(streamName string, record record.Record) *kinesis.PutRecordInput {
	return &kinesis.PutRecordInput{
		PartitionKey: aws.String(record.PartitionKey),
		StreamName:   aws.String(streamName),
		Data:         utils.CloneByteSlice(record.Data),
	}
}

func (producer *Producer) PutRecord(streamName string, record record.Record) (*kinesis.PutRecordOutput, error) {
	if len(record.Data) > MaxRecordSize {
		return nil, ErrRecordSizeExceeded
	}

	input := createPutRecordInput(streamName, record)

	producer.log.Info("PutRecord: Going to write data to kinesis")

	resp, err := producer.client.PutRecord(input)

	if err != nil {
		producer.log.Error(err)
	}

	return resp, err
}
