package kinesis

import (
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
}

// NewKCLWorkerFactory create a new KCL worker factory
func NewKCLWorkerFactory(configuration KCLConfiguration) *StandardKCLWorkerFactory {
	kclLibConf := config.NewKinesisClientLibConfigWithCredentials(
		configuration.ApplicationName,
		configuration.StreamName,
		configuration.RegionName,
		configuration.WorkerID,
		configuration.KinesisCredentials,
		configuration.DynamoDBCredentials,
	)
	if configuration.TableName != "" {
		kclLibConf.WithTableName(configuration.TableName)
	}
	kclworkerFactory := StandardKCLWorkerFactory{
		kinesisClientLibConf: kclLibConf,
	}
	return &kclworkerFactory
}

// NewWorker create a new worker based on kcl worker factory.
func (s *StandardKCLWorkerFactory) NewWorker(factory interfaces.IRecordProcessorFactory) *kclworker.Worker {
	return kclworker.NewWorker(factory, s.kinesisClientLibConf)
}
