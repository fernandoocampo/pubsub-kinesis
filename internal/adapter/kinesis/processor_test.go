package kinesis_test

import (
	"context"
	"encoding/json"
	"log"
	"testing"
	"time"

	awskinesis "github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/fernandoocampo/pubsub-kinesis/internal/adapter/kinesis"
	"github.com/stretchr/testify/assert"
	"github.com/vmware/vmware-go-kcl/clientlibrary/interfaces"
)

type TestMessage struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func TestProcessAnEventSuccesfully(t *testing.T) {
	expectedMessage := TestMessage{
		Key:   "one",
		Value: "usera",
	}
	customHandlerCreator := &handlerCreatorMock{}
	recordProcessorFactory := kinesis.NewRecordProcessorFactory(customHandlerCreator)
	customKCLWorker := kclWorkerMock{
		recordProcessorFactory: recordProcessorFactory,
		eventsToSend:           []string{`{"key": "one", "value": "usera"}`},
	}
	processor := kinesis.NewProcessor(&customKCLWorker)
	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	err := processor.Start(ctx)

	if err != nil {
		t.Error("unexpected error", err)
		t.FailNow()
	}
	assert.NotEmpty(t, customHandlerCreator.handler.events)
	assert.Len(t, customHandlerCreator.handler.events, 1)
	assert.Equal(t, expectedMessage, customHandlerCreator.handler.events[0])
}

func TestProcessThreeEventSuccesfully(t *testing.T) {
	expectedMessages := []TestMessage{
		{
			Key:   "one",
			Value: "usera",
		},
		{
			Key:   "two",
			Value: "userb",
		},
		{
			Key:   "three",
			Value: "userc",
		},
	}
	customHandlerCreator := &handlerCreatorMock{}
	recordProcessorFactory := kinesis.NewRecordProcessorFactory(customHandlerCreator)
	customKCLWorker := kclWorkerMock{
		recordProcessorFactory: recordProcessorFactory,
		eventsToSend:           []string{`{"key": "one", "value": "usera"}`, `{"key": "two", "value": "userb"}`, `{"key": "three", "value": "userc"}`},
	}
	processor := kinesis.NewProcessor(&customKCLWorker)
	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	err := processor.Start(ctx)

	if err != nil {
		t.Error("unexpected error", err)
		t.FailNow()
	}
	assert.NotEmpty(t, customHandlerCreator.handler.events)
	assert.Equal(t, expectedMessages, customHandlerCreator.handler.events)
}

type kclWorkerMock struct {
	processor              interfaces.IRecordProcessor
	recordProcessorFactory *kinesis.RecordProcessorFactory
	eventsToSend           []string
}

func (k *kclWorkerMock) Start() error {
	k.processor = k.recordProcessorFactory.CreateProcessor()
	now := time.Now()
	input := interfaces.ProcessRecordsInput{
		CacheEntryTime: &now,
		CacheExitTime:  &now,
		Records:        make([]*awskinesis.Record, 0),
		Checkpointer:   &recordProcessorCheckPointer{},
	}
	for _, v := range k.eventsToSend {
		newrecord := awskinesis.Record{
			Data: []byte(v),
		}
		input.Records = append(input.Records, &newrecord)
	}
	k.processor.ProcessRecords(&input)
	return nil
}

func (k *kclWorkerMock) Shutdown() {

}

type handlerCreatorMock struct {
	handler *handlerMock
}

// Create create a handlerMock
func (h *handlerCreatorMock) Create() kinesis.Handler {
	h.handler = &handlerMock{}
	return h.handler
}

type handlerMock struct {
	events []TestMessage
}

func (h *handlerMock) Handle(data []byte) {
	newTestMessage := TestMessage{}
	if err := json.Unmarshal(data, &newTestMessage); err != nil {
		log.Println("unexpected error", err)
		return
	}
	h.events = append(h.events, newTestMessage)
}

type recordProcessorCheckPointer struct{}

func (r *recordProcessorCheckPointer) Checkpoint(sequenceNumber *string) error {
	return nil
}
func (r *recordProcessorCheckPointer) PrepareCheckpoint(sequenceNumber *string) (interfaces.IPreparedCheckpointer, error) {
	return nil, nil
}
