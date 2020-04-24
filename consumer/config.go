package consumer

import (
	"github.com/Shopify/sarama"
	"github.com/tryfix/errors"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
)

type Config struct {
	Id               string
	GroupId          string
	BootstrapServers []string
	MetricsReporter  metrics.Reporter
	Logger           log.Logger
	options          *consumerOptions
	*sarama.Config
}

func (c *Config) validate() error {
	if err := c.Config.Validate(); err != nil {
		return err
	}

	if c.GroupId == `` {
		return errors.New(`k-stream.consumer.Config: Consumer.groupId cannot be empty`)
	}

	if len(c.BootstrapServers) < 1 {
		return errors.New(`k-stream.consumer.Config: Consumer.BootstrapServers cannot be empty`)
	}

	return nil
}

func NewConsumerConfig() *Config {
	c := new(Config)
	c.setDefaults()
	return c
}

func (c *Config) setDefaults() {
	c.Config = sarama.NewConfig()
	c.Config.Version = sarama.V2_3_0_0
	c.Consumer.Return.Errors = true
	c.ChannelBufferSize = 100
	c.MetricsReporter = metrics.NoopReporter()
	c.Logger = log.NewNoopLogger()
	c.options = new(consumerOptions)
	c.options.applyDefault()
}
