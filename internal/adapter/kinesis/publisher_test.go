package kinesis_test

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	pubsubkinesis "github.com/fernandoocampo/pubsub-kinesis/internal/adapter/kinesis"
	"github.com/stretchr/testify/assert"
)

func TestPublishWithPartitionKeySuccess(t *testing.T) {
	expectedReceivedRecords := 1
	rawMessage := `{"name":"fernando"}`
	streamName := "orders"
	kinesisRecordOutput := kinesis.PutRecordOutput{
		ShardId:        aws.String("123"),
		SequenceNumber: aws.String("321"),
		EncryptionType: aws.String("asdf23"),
	}
	awsKinesisClientMocked := awsKinesisMock{
		response:        &kinesisRecordOutput,
		receivedRecords: make([]*kinesis.PutRecordInput, 0),
	}
	partitionKey := "ddfasdf2343sfsd434sfs"
	message := []byte(rawMessage)
	kinesisClient := pubsubkinesis.NewClient(streamName, &awsKinesisClientMocked)

	err := kinesisClient.Publish(message, partitionKey)

	assert.NoError(t, err)
	assert.NotEmpty(t, awsKinesisClientMocked.receivedRecords)
	assert.Equal(t, expectedReceivedRecords, len(awsKinesisClientMocked.receivedRecords))
	receivedRecord := awsKinesisClientMocked.receivedRecords[0]
	assert.Equal(t, rawMessage, string(receivedRecord.Data))
	assert.NotEmpty(t, receivedRecord.PartitionKey)
	assert.Equal(t, partitionKey, *receivedRecord.ExplicitHashKey)
}

func TestPublishWithOutPartitionKeySuccess(t *testing.T) {
	expectedReceivedRecords := 1
	rawMessage := `{"name":"fernando"}`
	streamName := "orders"
	kinesisRecordOutput := kinesis.PutRecordOutput{
		ShardId:        aws.String("123"),
		SequenceNumber: aws.String("321"),
		EncryptionType: aws.String("asdf23"),
	}
	awsKinesisClientMocked := awsKinesisMock{
		response: &kinesisRecordOutput,
	}
	partitionKey := ""
	message := []byte(rawMessage)
	kinesisClient := pubsubkinesis.NewClient(streamName, &awsKinesisClientMocked)

	err := kinesisClient.Publish(message, partitionKey)

	assert.NoError(t, err)
	assert.NotEmpty(t, awsKinesisClientMocked.receivedRecords)
	assert.Equal(t, expectedReceivedRecords, len(awsKinesisClientMocked.receivedRecords))
	receivedRecord := awsKinesisClientMocked.receivedRecords[0]
	assert.Equal(t, rawMessage, string(receivedRecord.Data))
	assert.NotEmpty(t, receivedRecord.PartitionKey)
	assert.Empty(t, receivedRecord.ExplicitHashKey)
}

func TestPublishWithOutPartitionKeyFailed(t *testing.T) {
	expectedReceivedRecords := 1
	rawMessage := `{"name":"fernando"}`
	streamName := "orders"
	kinesisRecordOutput := kinesis.PutRecordOutput{
		ShardId:        aws.String("123"),
		SequenceNumber: aws.String("321"),
		EncryptionType: aws.String("asdf23"),
	}
	awsKinesisClientMocked := awsKinesisMock{
		response: &kinesisRecordOutput,
		err:      errors.New("unexpected error"),
	}
	partitionKey := ""
	message := []byte(rawMessage)
	kinesisClient := pubsubkinesis.NewClient(streamName, &awsKinesisClientMocked)

	err := kinesisClient.Publish(message, partitionKey)

	assert.Error(t, err)
	assert.NotEmpty(t, awsKinesisClientMocked.receivedRecords)
	assert.Equal(t, expectedReceivedRecords, len(awsKinesisClientMocked.receivedRecords))
	receivedRecord := awsKinesisClientMocked.receivedRecords[0]
	assert.Equal(t, rawMessage, string(receivedRecord.Data))
	assert.NotEmpty(t, receivedRecord.PartitionKey)
	assert.Empty(t, receivedRecord.ExplicitHashKey)
}

type awsKinesisMock struct {
	receivedRecords []*kinesis.PutRecordInput
	response        *kinesis.PutRecordOutput
	err             error
}

func (a *awsKinesisMock) PutRecord(record *kinesis.PutRecordInput) (*kinesis.PutRecordOutput, error) {
	a.receivedRecords = append(a.receivedRecords, record)
	if a.err != nil {
		return nil, a.err
	}
	return a.response, nil
}
