package kinesis

import (
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/vmware/vmware-go-kcl/clientlibrary/config"
	"github.com/vmware/vmware-go-kcl/clientlibrary/interfaces"
	kclworker "github.com/vmware/vmware-go-kcl/clientlibrary/worker"
)

// KCLWorker defines behavior for KCL workers
type KCLWorker interface {
	Start() error
	Shutdown()
}

// StandardKCLWorkerFactory it is the standard procedure to create a KCL workers/
type StandardKCLWorkerFactory struct {
	kinesisClientLibConf *config.KinesisClientLibConfiguration
	kinesisSvc           kinesisiface.KinesisAPI
}

// NewKCLWorkerFactory create a new KCL worker factory
func NewKCLWorkerFactory(configuration KCLConfiguration) *StandardKCLWorkerFactory {
	// kclLibConf := config.NewKinesisClientLibConfigWithCredentials(
	// 	configuration.ApplicationName,
	// 	configuration.StreamName,
	// 	configuration.RegionName,
	// 	configuration.WorkerID,
	// 	configuration.KinesisCredentials,
	// 	configuration.DynamoDBCredentials,
	// )
	kclLibConf := config.NewKinesisClientLibConfig(
		configuration.ApplicationName,
		configuration.StreamName,
		configuration.RegionName,
		configuration.WorkerID,
	)
	if configuration.KinesisCredentials != nil {
		kclLibConf.KinesisCredentials = configuration.KinesisCredentials
	}
	if configuration.DynamoDBCredentials != nil {
		kclLibConf.DynamoDBCredentials = configuration.DynamoDBCredentials
	}
	// kclLibConf.WithInitialPositionInStream(config.DefaultInitialPositionInStream)
	if configuration.TableName != "" {
		kclLibConf.WithTableName(configuration.TableName)
	}
	if configuration.DynamoDBEndpoint != "" {
		kclLibConf.WithDynamoDBEndpoint(configuration.DynamoDBEndpoint)
	}
	if configuration.KinesisEndpoint != "" {
		kclLibConf.WithKinesisEndpoint(configuration.KinesisEndpoint)
	}
	kclLibConf.InitialPositionInStream = config.LATEST
	kclworkerFactory := StandardKCLWorkerFactory{
		kinesisClientLibConf: kclLibConf,
	}
	return &kclworkerFactory
}

// NewWorker create a new worker based on kcl worker factory.
func (s *StandardKCLWorkerFactory) NewWorker(factory interfaces.IRecordProcessorFactory, kinesisSvc kinesisiface.KinesisAPI) *kclworker.Worker {
	newWorker := kclworker.NewWorker(factory, s.kinesisClientLibConf)
	if kinesisSvc != nil {
		newWorker.WithKinesis(s.kinesisSvc)
	}
	return newWorker
}
