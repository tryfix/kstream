package kstream

import (
	"github.com/tryfix/kstream/admin"
	"github.com/tryfix/kstream/backend"
	"github.com/tryfix/kstream/backend/memory"
	"github.com/tryfix/kstream/consumer"
	"github.com/tryfix/kstream/kstream/changelog"
	"github.com/tryfix/kstream/kstream/encoding"
	"github.com/tryfix/kstream/kstream/offsets"
	"github.com/tryfix/kstream/kstream/store"
	"github.com/tryfix/kstream/producer"
)

type DefaultBuilders struct {
	Producer          producer.Builder
	changelog         changelog.Builder
	Consumer          consumer.Builder
	PartitionConsumer consumer.PartitionConsumerBuilder
	Store             store.Builder
	Backend           backend.Builder
	StateStore        store.StateStoreBuilder
	OffsetManager     offsets.Manager
	KafkaAdmin        admin.KafkaAdmin
	configs           *StreamBuilderConfig
}

func (dbs *DefaultBuilders) build(options ...BuilderOption) {
	// apply options
	for _, option := range options {
		option(dbs)
	}

	// default backend builder will be memory
	if dbs.configs.Store.BackendBuilder == nil {
		backendBuilderConfig := memory.NewConfig()
		backendBuilderConfig.Logger = dbs.configs.Logger
		backendBuilderConfig.MetricsReporter = dbs.configs.MetricsReporter
		dbs.Backend = memory.Builder(backendBuilderConfig)
		dbs.configs.Store.BackendBuilder = dbs.Backend
	}

	dbs.Backend = dbs.configs.Store.BackendBuilder
	dbs.configs.Store.BackendBuilder = dbs.Backend

	dbs.Store = func(name string, keyEncoder encoding.Builder, valEncoder encoding.Builder, options ...store.Options) (store.Store, error) {
		return store.NewStore(name, keyEncoder(), valEncoder(), append(
			options,
			store.WithBackendBuilder(dbs.configs.Store.BackendBuilder),
			store.WithLogger(dbs.configs.Logger),
		)...)
	}

	if dbs.Producer == nil {
		pool, err := producer.NewPool(dbs.configs.Producer.Pool.NumOfWorkers, func(options *producer.Config) (producer.Producer, error) {
			options = dbs.configs.Producer
			options.BootstrapServers = dbs.configs.BootstrapServers
			options.Logger = dbs.configs.Logger
			options.MetricsReporter = dbs.configs.MetricsReporter
			return producer.NewProducer(options)
		})
		if err != nil {
			dbs.configs.Logger.Fatal(err)
		}
		dbs.Producer = func(options *producer.Config) (producer.Producer, error) {
			return pool, nil
		}
	}

	if dbs.Consumer == nil {
		dbs.Consumer = consumer.NewBuilder()
	}
	dbs.Consumer.Config().GroupId = dbs.configs.ApplicationId
	dbs.Consumer.Config().BootstrapServers = dbs.configs.BootstrapServers
	dbs.Consumer.Config().MetricsReporter = dbs.configs.MetricsReporter
	dbs.Consumer.Config().Logger = dbs.configs.Logger

	if dbs.OffsetManager == nil {
		dbs.OffsetManager = offsets.NewManager(&offsets.Config{
			Config:           dbs.configs.Config,
			BootstrapServers: dbs.configs.BootstrapServers,
			Logger:           dbs.configs.Logger,
		})
	}

	if dbs.KafkaAdmin == nil {
		dbs.KafkaAdmin = admin.NewKafkaAdmin(dbs.configs.BootstrapServers,
			admin.WithKafkaVersion(dbs.configs.Consumer.Version),
			admin.WithLogger(dbs.configs.Logger),
		)
	}

	if dbs.PartitionConsumer == nil {
		dbs.PartitionConsumer = consumer.NewPartitionConsumerBuilder()
	}
	dbs.PartitionConsumer.Config().BootstrapServers = dbs.configs.BootstrapServers
	dbs.PartitionConsumer.Config().MetricsReporter = dbs.configs.MetricsReporter
	dbs.PartitionConsumer.Config().Logger = dbs.configs.Logger

}
