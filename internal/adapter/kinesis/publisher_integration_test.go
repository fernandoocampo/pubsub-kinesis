package kinesis_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	awskinesis "github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/fernandoocampo/pubsub-kinesis/internal/adapter/kinesis"
)

func TestPublishMessage(t *testing.T) {
	// if !*integration {
	// 	t.Skip("skipping integration test")
	// }
	// GIVEN
	message := "this is a message"
	partitionKey := "ddfasdf2343sfsd434sfs"
	streamName := "messages"
	awsSession, err := createAWSSession(*awsRegionName, *kinesisEndpoint)
	if err != nil {
		t.Errorf("unexpected error creating aws session: %s", err)
		t.FailNow()
	}
	kinesisClient := awskinesis.New(awsSession)
	client := kinesis.NewClient(streamName, kinesisClient)

	publishErr := client.Publish([]byte(message), partitionKey)
	if publishErr != nil {
		t.Errorf("unexpected error publishing message: %s", publishErr)
		t.FailNow()
	}
}

func TestAnother(t *testing.T) {
	stream := "messages"
	region := "us-west-2"
	endpoint := "http://localhost:4566"
	s := session.New(&aws.Config{Region: aws.String(region), Endpoint: aws.String(endpoint)})
	kc := awskinesis.New(s)

	streamName := aws.String(stream)

	// out, err := kc.CreateStream(&awskinesis.CreateStreamInput{
	// 	ShardCount: aws.Int64(1),
	// 	StreamName: streamName,
	// })
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Printf("%v\n", out)

	if err := kc.WaitUntilStreamExists(&awskinesis.DescribeStreamInput{StreamName: streamName}); err != nil {
		panic(err)
	}

	streams, err := kc.DescribeStream(&awskinesis.DescribeStreamInput{StreamName: streamName})
	if err != nil {
		panic(err)
	}
	fmt.Printf("%v\n", streams)

	putOutput, err := kc.PutRecord(&awskinesis.PutRecordInput{
		Data:         []byte("hoge"),
		StreamName:   streamName,
		PartitionKey: aws.String("key1"),
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("%v\n", putOutput)
	mytimehascome := time.After(4 * time.Second)
	<-mytimehascome
}
