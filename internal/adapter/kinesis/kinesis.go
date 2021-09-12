package kinesis

import (
	"errors"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	vmwarekcl "github.com/vmware/vmware-go-kcl/clientlibrary/utils"
)

// API defines kinesis client behavior
type API interface {
	PutRecord(*kinesis.PutRecordInput) (*kinesis.PutRecordOutput, error)
}

// Client contains data to connect to kinesis streaming service
type Client struct {
	streamName    string
	kinesisClient API
}

// NewClient creates a new kinesis client.
func NewClient(streamName string, kinesisClient API) *Client {
	log.Println("msg", "creating new kinesis client")

	newClient := Client{
		kinesisClient: kinesisClient,
	}

	return &newClient
}

// Publish sends a new message into the stream.
func (c *Client) Publish(message []byte, partitionKey string) error {
	log.Println("publishing a new message")
	input := c.buildPutRecordInput(message, partitionKey)

	log.Println(
		"msg", "publishing new message",
		"stream", c.streamName,
		"generated partition key", input.PartitionKey,
		"explicit hash key", partitionKey,
	)

	output, err := c.kinesisClient.PutRecord(input)
	if err != nil {
		log.Println("msg", "error in publishing a message", "error", err)
		return errors.New("error in publishing a message into kinesis stream")
	}

	log.Println("msg", "message was published into kinesis", "sequence", output.SequenceNumber, "shardid", output.ShardId)
	return nil
}

// buildPutRecordInput
func (c *Client) buildPutRecordInput(message []byte, partitionKey string) *kinesis.PutRecordInput {
	input := kinesis.PutRecordInput{
		Data:         message,
		StreamName:   aws.String(c.streamName),
		PartitionKey: aws.String(vmwarekcl.RandStringBytesMaskImpr(10)),
	}
	if partitionKey != "" {
		input.ExplicitHashKey = &partitionKey
	}
	return &input
}
