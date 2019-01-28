package kinesis_producer

import (
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/mfarhan08/kinesis-producer/record"
	"github.com/mfarhan08/kinesis-producer/utils"
	"github.com/sirupsen/logrus"
)

const (
	MaxRecordSize        = 1 << 20
	MaxPutRequestSize    = MaxRecordSize * 5
	MaxRecordsPerRequest = 500
)

var (
	ErrPutRequestSizeExceeded    = errors.New("kinesis: Max put request size exceeded.")
	ErrRecordSizeExceeded        = errors.New("kinesis: Individual record size exceeded.")
	ErrRecordsPerRequestExceeded = errors.New("kinesis: Max records per request exceeded.")
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

func createPutRecordsRequestEntry(record *record.Record) (*kinesis.PutRecordsRequestEntry, error) {
	if len(record.Data) > MaxRecordSize {
		return nil, ErrRecordSizeExceeded
	}
	return &kinesis.PutRecordsRequestEntry{
		Data:         utils.CloneByteSlice(record.Data),
		PartitionKey: aws.String(record.PartitionKey),
	}, nil
}

func createPutRecordsInput(streamName string, records []*record.Record) (*kinesis.PutRecordsInput, error) {
	if len(records) > MaxRecordsPerRequest {
		return nil, ErrRecordsPerRequestExceeded
	}

	putRecordsInput := kinesis.PutRecordsInput{
		StreamName: aws.String(streamName),
	}

	var totalSize int
	for _, record := range records {

		recordsRequestEntry, err := createPutRecordsRequestEntry(record)

		if err != nil {
			return nil, err
		}

		putRecordsInput.Records = append(putRecordsInput.Records, recordsRequestEntry)

		totalSize += len(record.Data)
		if totalSize > MaxPutRequestSize {
			return nil, ErrPutRequestSizeExceeded
		}
	}
	return &putRecordsInput, nil
}

func (producer *Producer) PutRecords(records []*record.Record) (*kinesis.PutRecordsOutput, error) {

	putRecordsInput, err := createPutRecordsInput(producer.streamName, records)

	if err != nil {
		producer.log.Error(err)
		return nil, err
	}

	resp, err := producer.client.PutRecords(putRecordsInput)

	if err != nil {
		producer.log.Error(err)
	}
	return resp, err
}

func createPutRecordInput(streamName string, record *record.Record) (*kinesis.PutRecordInput, error) {
	if len(record.Data) > MaxPutRequestSize {
		return nil, ErrRecordSizeExceeded
	}
	return &kinesis.PutRecordInput{
		PartitionKey: aws.String(record.PartitionKey),
		StreamName:   aws.String(streamName),
		Data:         utils.CloneByteSlice(record.Data),
	}, nil
}

func (producer *Producer) PutRecord(record *record.Record) (*kinesis.PutRecordOutput, error) {
	input, err := createPutRecordInput(producer.streamName, record)

	if err != nil {
		producer.log.Error(err)
		return nil, err
	}

	producer.log.Printf("PutRecord: Stream %s\n", producer.streamName)

	resp, err := producer.client.PutRecord(input)

	if err != nil {
		producer.log.Error(err)
	}

	return resp, err
}
