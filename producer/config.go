package producer

import (
	"github.com/Shopify/sarama"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
)

type Config struct {
	Id string
	*sarama.Config
	Pool struct {
		NumOfWorkers int
	}
	BootstrapServers []string
	RequiredAcks     RequiredAcks
	Partitioner      Partitioner
	Logger           log.Logger
	MetricsReporter  metrics.Reporter
}

func NewConfig() *Config {
	c := new(Config)
	c.setDefaults()
	return c
}

func (c *Config) validate() error {
	if err := c.Config.Validate(); err != nil {
		return err
	}
	return nil
}

func (c *Config) setDefaults() {
	c.Config = sarama.NewConfig()
	c.Producer.RequiredAcks = sarama.RequiredAcks(WaitForAll)
	c.Producer.Return.Errors = true
	c.Producer.Return.Successes = true
	c.Logger = log.NewNoopLogger()
	//c.Config.Version = sarama.V2_3_0_0
	c.MetricsReporter = metrics.NoopReporter()

	c.Producer.Compression = sarama.CompressionSnappy

	if c.Partitioner == Manual {
		c.Producer.Partitioner = sarama.NewManualPartitioner
	}

	if c.Partitioner == HashBased {
		c.Producer.Partitioner = sarama.NewHashPartitioner
	}

	if c.Partitioner == Random {
		c.Producer.Partitioner = sarama.NewRandomPartitioner
	}
}
