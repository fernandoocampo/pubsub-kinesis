package kinesis

import (
	"context"
	"errors"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/vmware/vmware-go-kcl/clientlibrary/interfaces"
)

// Consumer defines behavior for kinesis consumers
type Consumer interface{}

// KCLConfiguration contains data required for the Kinesis Client Library
type KCLConfiguration struct {
	KinesisCredentials  *credentials.Credentials
	DynamoDBCredentials *credentials.Credentials
	TableName           string
	ApplicationName     string
	StreamName          string
	RegionName          string
	WorkerID            string
	KinesisEndpoint     string
	DynamoDBEndpoint    string
	// failoverTimeMillis leases not renewed within this period will be claimed by others
	FailoverTimeMillis int
	// leaseRefreshPeriodMillis is the period before the end of lease during which a lease is refreshed by the owner.
	LeaseRefreshPeriodMillis int
	// maxRecords Max records to read per Kinesis getRecords() call
	MaxRecords int
	// idleTimeBetweenReadsInMillis Idle time between calls to fetch data from Kinesis
	// for how long the consumer should sleep if no records are returned from the call to
	IdleTimeBetweenReadsInMillis int
	// callProcessRecordsEvenForEmptyRecordList Call the IRecordProcessor::processRecords() API even if
	// GetRecords returned an empty record list.
	CallProcessRecordsEvenForEmptyRecordList bool
	// parentShardPollIntervalMillis Wait for this long between polls to check if parent shards are done.
	// interval in milliseconds between polling to check for parent shard completion.
	// polling frequently will take up more DynamoDB IOPS (when there are leases for shards waiting on
	// completion of parent shards). IOPS - Input/Output Operations Per Second
	ParentShardPollIntervalMillis int
	// shardSyncIntervalMillis Time between tasks to sync leases and Kinesis shards.
	// e.g. wait for this long between shard sync tasks.
	ShardSyncIntervalMillis int
	// cleanupTerminatedShardsBeforeExpiry Clean up shards we've finished processing (don't wait until they expire in Kinesis).
	// Keeping leases takes some tracking/resources (e.g. they need to be renewed, assigned), so by
	// default we try to delete the ones we don't need any longer.
	CleanupTerminatedShardsBeforeExpiry bool
	// TaskBackoffTimeMillis Backoff period when tasks encounter an exception
	TaskBackoffTimeMillis int
	// ValidateSequenceNumberBeforeCheckpointing whether KCL should validate client provided sequence numbers
	ValidateSequenceNumberBeforeCheckpointing bool
	// ShutdownGraceMillis The number of milliseconds before graceful shutdown terminates forcefully
	ShutdownGraceMillis int
	// Max leases this Worker can handle at a time
	// This can be useful to avoid overloading (and thrashing) a worker when a host has resource constraints
	// or during deployment.
	// NOTE: Setting this to a low value can cause data loss if workers are not able to pick up all shards in the
	// stream due to the max limit.
	MaxLeasesForWorker int
	// Max leases to steal at one time (for load balancing)
	// Setting this to a higher number can allow for faster load convergence (e.g. during deployments, cold starts),
	// but can cause higher churn in the system.
	MaxLeasesToStealAtOneTime int
	// Read capacity to provision when creating the lease table (dynamoDB).
	// The Amazon DynamoDB table used for tracking leases will be provisioned with this read capacity.
	InitialLeaseTableReadCapacity int
	// Write capacity to provision when creating the lease table.
	// the table used for tracking leases will be provisioned with this write capacity.
	InitialLeaseTableWriteCapacity int
	// Worker should skip syncing shards and leases at startup if leases are present
	// This is useful for optimizing deployments to large fleets working on a stable stream.
	// The Worker will skip shard sync during initialization if there are one or more leases in the lease table. This
	// assumes that the shards and leases are in-sync. This enables customers to choose faster startup times (e.g.
	// during incremental deployments of an application).
	SkipShardSyncAtWorkerInitializationIfLeasesExist bool

	// Logger used to log message.
	// Logger logger.Logger

	// EnableLeaseStealing turns on lease stealing
	// Lease stealing defaults to false for backwards compatibility.
	EnableLeaseStealing bool

	// LeaseStealingIntervalMillis The number of milliseconds between rebalance tasks
	LeaseStealingIntervalMillis int

	// LeaseStealingClaimTimeoutMillis The number of milliseconds to wait before another worker can aquire a claimed shard
	// Number of milliseconds to wait before another worker can aquire a claimed shard
	LeaseStealingClaimTimeoutMillis int

	// LeaseSyncingTimeInterval The number of milliseconds to wait before syncing with lease table (dynamoDB)
	// Number of milliseconds to wait before syncing with lease table (dynamodDB)
	LeaseSyncingTimeIntervalMillis int
}

// Handler defines behavior to process the message that was sent within the event.
type Handler interface {
	Handle(data []byte)
}

// RecordProcessor defines a record processor for records provided by kinesis.
type RecordProcessor struct {
	handler Handler
}

// Initialize Invoked by the Amazon Kinesis Client Library before data records are delivered to the RecordProcessor instance
func (r *RecordProcessor) Initialize(input *interfaces.InitializationInput) {
	log.Println(
		"level", "DEBUG",
		"msg", "initializing record processor",
		"shard", input.ShardId,
		"checkpoint", aws.StringValue(input.ExtendedSequenceNumber.SequenceNumber),
	)
}

// ProcessRecords Process data records. The Amazon Kinesis Client Library will invoke this method to deliver data records to the
func (r *RecordProcessor) ProcessRecords(input *interfaces.ProcessRecordsInput) {
	log.Println("level", "debug", "msg", "processing records")
	// don't process empty record
	if len(input.Records) == 0 {
		return
	}

	for _, v := range input.Records {
		r.handler.Handle(v.Data)
	}

	// checkpoint it after processing this batch.
	// Especially, for processing de-aggregated KPL records, checkpointing has to happen at the end of batch
	// because de-aggregated records share the same sequence number.
	lastRecordSequenceNumber := input.Records[len(input.Records)-1].SequenceNumber
	diff := input.CacheExitTime.Sub(*input.CacheEntryTime)
	log.Println("level", "debug", "checkpoint progress at", lastRecordSequenceNumber, "millisBehindLatest", input.MillisBehindLatest, "kclProcessTime", diff)
	err := input.Checkpointer.Checkpoint(lastRecordSequenceNumber)
	if err != nil {
		// TODO check if we need to retry this
		log.Println("level", "error", "msg", "error checkpointing progress", "error", err)
	}
}

// Shutdown Invoked by the Amazon Kinesis Client Library to indicate it will no longer send data records to this
// RecordProcessor instance.
func (r *RecordProcessor) Shutdown(shutdownInput *interfaces.ShutdownInput) {
	log.Println(
		"level", "debug",
		"msg", "shutting record processor down",
		"reason", aws.StringValue(interfaces.ShutdownReasonMessage(shutdownInput.ShutdownReason)),
	)
	if shutdownInput.ShutdownReason == interfaces.TERMINATE {
		shutdownInput.Checkpointer.Checkpoint(nil)
	}
}

// HandlerCreator defines behavior to create instances of Handler
type HandlerCreator interface {
	Create() Handler
}

// RecordProcessorFactory defines a factor to create record processors.
type RecordProcessorFactory struct {
	handlerCreator HandlerCreator
}

// NewRecordProcessorFactory creates a new record processor factory
func NewRecordProcessorFactory(handlerCreator HandlerCreator) *RecordProcessorFactory {
	log.Println("level", "INFO", "msg", "creating kinesis record processor factory")
	newRecordProcessorFactory := RecordProcessorFactory{
		handlerCreator: handlerCreator,
	}
	return &newRecordProcessorFactory
}

// CreateProcessor Returns a record processor to be used for processing data records for a (assigned) shard.
func (r *RecordProcessorFactory) CreateProcessor() interfaces.IRecordProcessor {
	log.Println("level", "INFO", "method", "RecordProcessorFactory.CreateProcessor", "msg", "creating kinesis record processor")
	newRecordProcessor := RecordProcessor{
		handler: r.handlerCreator.Create(),
	}
	return &newRecordProcessor
}

// Processor defines a worker to process events that comes from kinesis
type Processor struct {
	kclWorker KCLWorker
}

// NewProcessor creates a new kinesis processor using kcl.
func NewProcessor(kclWorker KCLWorker) *Processor {
	log.Println("level", "INFO", "method", "kinesis.NewProcessor", "msg", "creating kinesis processor")

	newProcessor := Processor{
		kclWorker: kclWorker,
	}

	return &newProcessor
}

// Start starts the kinesis processor which starts the kcl worker.
func (p *Processor) Start(ctx context.Context) error {
	log.Println("level", "INFO", "msg", "starting kinesis procesor")
	select {
	case <-ctx.Done():
		p.Shutdown()
		return nil
	default:
		err := p.kclWorker.Start()
		if err != nil {
			log.Println("level", "ERROR", "msg", "something went wrong when trying to start the KCLWorker", "error", err)
			return errors.New("something went wrong when trying to start the KCLWorker")
		}
		log.Println("level", "INFO", "msg", "kinesis processor started")
	}
	return nil
}

// Shutdown turn off the processor
func (p *Processor) Shutdown() {
	log.Println("level", "INFO", "msg", "shutting down kinesis procesor")
	p.kclWorker.Shutdown()
	log.Println("level", "INFO", "msg", "kinesis processor has been stopped")
}
