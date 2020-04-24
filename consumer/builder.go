package consumer

import (
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
)

type BuilderOption func(config *Config)

func BuilderWithId(id string) BuilderOption {
	return func(config *Config) {
		config.Id = id
	}
}

func BuilderWithOptions(options ...Option) BuilderOption {
	return func(config *Config) {
		config.options.apply(options...)
	}
}

func BuilderWithGroupId(id string) BuilderOption {
	return func(config *Config) {
		config.GroupId = id
	}
}

func BuilderWithMetricsReporter(reporter metrics.Reporter) BuilderOption {
	return func(config *Config) {
		config.MetricsReporter = reporter
	}
}

func BuilderWithLogger(logger log.Logger) BuilderOption {
	return func(config *Config) {
		config.Logger = logger
	}
}

type Builder interface {
	Config() *Config
	Build(options ...BuilderOption) (Consumer, error)
}

type builder struct {
	config *Config
}

func NewBuilder() Builder {
	return &builder{
		config: NewConsumerConfig(),
	}
}

func (b *builder) Config() *Config {
	return b.config
}

func (b *builder) Build(options ...BuilderOption) (Consumer, error) {
	conf := *b.config
	for _, option := range options {
		option(&conf)
	}
	return NewConsumer(&conf)
}

type PartitionConsumerBuilder interface {
	Config() *Config
	Build(options ...BuilderOption) (PartitionConsumer, error)
}

type partitionConsumerBuilder struct {
	config *Config
}

func NewPartitionConsumerBuilder() PartitionConsumerBuilder {
	return &partitionConsumerBuilder{
		config: NewConsumerConfig(),
	}
}

func (b *partitionConsumerBuilder) Config() *Config {
	return &*b.config
}

func (b *partitionConsumerBuilder) Configure(c *Config) PartitionConsumerBuilder {
	return &partitionConsumerBuilder{
		config: c,
	}
}

func (b *partitionConsumerBuilder) Build(options ...BuilderOption) (PartitionConsumer, error) {
	conf := *b.config
	for _, option := range options {
		option(&conf)
	}
	return NewPartitionConsumer(&conf)
}
