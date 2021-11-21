package kinesis_test

import (
	"context"
	"log"
	"sync"
	"testing"
	"time"

	awskinesis "github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/fernandoocampo/pubsub-kinesis/internal/adapter/kinesis"
	"github.com/stretchr/testify/assert"
)

func TestProcessingMessages(t *testing.T) {
	// if !*integration {
	// 	t.Skip("skipping integration test")
	// }
	// GIVEN
	message := "this is a message"
	expectedMessage := "this is a message"
	streamName := "messages"
	partitionKey := "ddfasdf2343sfsd434sfs"
	kclConfiguration := kinesis.KCLConfiguration{
		TableName:        "message-test-checkpoint",
		ApplicationName:  "message-test",
		StreamName:       streamName,
		RegionName:       *awsRegionName,
		WorkerID:         "1",
		DynamoDBEndpoint: *dynamoDBEndpoint,
		KinesisEndpoint:  *kinesisEndpoint,
		// KinesisCredentials: credentials.NewStaticCredentials("id", "sd", "idsd"),
		// KinesisCredentials:  credentials.AnonymousCredentials,
		// DynamoDBCredentials: credentials.AnonymousCredentials,
	}
	kinesisSession, err := createAWSSession(*awsRegionName, *kinesisEndpoint)
	if err != nil {
		t.Errorf("unexpected error creating aws session: %s", err)
		t.FailNow()
	}
	kinesisSession2, err := createAWSSession(*awsRegionName, *kinesisEndpoint)
	if err != nil {
		t.Errorf("unexpected error creating aws session: %s", err)
		t.FailNow()
	}
	// dynamoDBSession, err := createAWSSession(*awsRegionName, *dynamoDBEndpoint)
	kinesisClient := awskinesis.New(kinesisSession)
	kinesisClient2 := awskinesis.New(kinesisSession2)
	// dynamoDB := dynamodb.NewClient(awsSession)
	workerFactory := kinesis.NewKCLWorkerFactory(kclConfiguration)
	handlerFactory := newHandlerFactoryTest()
	recordFactory := kinesis.NewRecordProcessorFactory(handlerFactory)
	worker := workerFactory.NewWorker(recordFactory, kinesisClient)
	proccesor := kinesis.NewProcessor(worker)
	ctx := context.TODO()
	processorErr := proccesor.Start(ctx)
	if processorErr != nil {
		t.Errorf("unexpected error starting processor: %s", processorErr)
		t.FailNow()
	}
	defer proccesor.Shutdown()

	time.Sleep(5 * time.Second)
	publisher := createKinesisPublisher(streamName, kinesisClient2)
	handlerFactory.handler.wg.Add(1)
	publisher.Publish([]byte(message), partitionKey)
	handlerFactory.handler.wg.Wait()

	assert.Equal(t, 1, len(handlerFactory.handler.storage))
	assert.Equal(t, expectedMessage, string(handlerFactory.handler.storage[0]))
}

func publishMessage(t *testing.T, client *kinesis.PublisherClient, message []byte) {
	t.Helper()
	partitionKey := "ddfasdf2343sfsd434sfs"
	publishErr := client.Publish([]byte(message), partitionKey)
	if publishErr != nil {
		t.Errorf("unexpected error publishing message: %s", publishErr)
		t.FailNow()
	}
}

func createKinesisPublisher(streamName string, kinesisClient *awskinesis.Kinesis) *kinesis.PublisherClient {
	client := kinesis.NewClient(streamName, kinesisClient)
	return client
}

type handlerFactoryTest struct {
	handler *handlerTest
}

func newHandlerFactoryTest() *handlerFactoryTest {
	handler := handlerTest{
		storage: make([][]byte, 0),
		lock:    sync.Mutex{},
		wg:      sync.WaitGroup{},
	}
	handlerFactory := handlerFactoryTest{
		handler: &handler,
	}
	return &handlerFactory
}

func (h *handlerFactoryTest) Create() kinesis.Handler {
	return h.handler
}

type handlerTest struct {
	storage [][]byte
	lock    sync.Mutex
	wg      sync.WaitGroup
}

func (h *handlerTest) Handle(message []byte) {
	log.Println("handle new message", string(message))
	defer h.wg.Done()
	h.lock.Lock()
	defer h.lock.Unlock()
	h.storage = append(h.storage, message)
}
