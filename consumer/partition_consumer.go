package consumer

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"github.com/tryfix/errors"
	"github.com/tryfix/kstream/data"
	"github.com/tryfix/kstream/k-stream/offsets"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
	"time"
)

type PartitionConsumer interface {
	Consume(topic string, partition int32, offset Offset) (<-chan Event, error)
	Errors() <-chan *Error
	Close() error
	Id() string
}

type partitionConsumer struct {
	id                string
	offsets           offsets.Manager
	consumerEvents    chan Event
	consumerErrors    chan *Error
	consumer          sarama.Consumer
	partitionConsumer sarama.PartitionConsumer
	logger            log.Logger
	metrics           struct {
		consumerBuffer    metrics.Gauge
		consumerBufferMax metrics.Gauge
		endToEndLatency   metrics.Observer
	}
	closing chan bool
	closed  chan bool
}

func NewPartitionConsumer(c *Config) (PartitionConsumer, error) {
	if err := c.Validate(); err != nil {
		log.Fatal(err)
		return nil, err
	}

	offsetManager := offsets.NewManager(&offsets.Config{
		Config:           c.Config,
		BootstrapServers: c.BootstrapServers,
		Logger:           c.Logger,
	})

	consumer, err := sarama.NewConsumer(c.BootstrapServers, c.Config)
	if err != nil {
		return nil, errors.WithPrevious(err, `new consumer failed `)
	}

	pc := &partitionConsumer{
		id:             c.Id,
		offsets:        offsetManager,
		consumer:       consumer,
		consumerEvents: make(chan Event, c.ChannelBufferSize),
		consumerErrors: make(chan *Error, 1),
		closed:         make(chan bool, 1),
		closing:        make(chan bool, 1),
		logger:         c.Logger.NewLog(log.Prefixed(`partition-consumer`)),
	}

	labels := []string{`topic`, `partition`}
	pc.metrics.consumerBuffer = c.MetricsReporter.Gauge(metrics.MetricConf{
		Path:   `k_stream_partition_consumer_buffer`,
		Labels: append(labels, []string{`type`}...),
	})
	pc.metrics.consumerBufferMax = c.MetricsReporter.Gauge(metrics.MetricConf{
		Path:   `k_stream_partition_consumer_buffer_max`,
		Labels: append(labels, []string{`type`}...),
	})
	pc.metrics.endToEndLatency = c.MetricsReporter.Observer(metrics.MetricConf{
		Path:   `k_stream_partition_consumer_end_to_end_latency_microseconds`,
		Labels: labels,
	})

	return pc, nil
}

func (c *partitionConsumer) Consume(topic string, partition int32, offset Offset) (<-chan Event, error) {

	partitionStart, err := c.offsets.GetOffsetOldest(topic, partition)
	if err != nil {
		return nil, errors.WithPrevious(err, fmt.Sprintf(`cannot get oldest offset for %s[%d]`, topic, partition))
	}

	partitionEnd, err := c.offsets.GetOffsetLatest(topic, partition)
	if err != nil {
		return nil, errors.WithPrevious(err, fmt.Sprintf(`cannot get latest latest for %s[%d]`, topic, partition))
	}

	// partition is empty
	if offset == Offset(sarama.OffsetNewest) || partitionEnd == 0 || partitionStart == partitionEnd || offset == Offset(partitionEnd-1) {
		// change the offset to default offset
		offset = Offset(sarama.OffsetOldest)
		c.consumerEvents <- &PartitionEnd{
			tps: []TopicPartition{{
				Topic:     topic,
				Partition: partition,
			}},
		}

		// if offset is valid always request the next offset
		if offset > 0 {
			offset += 1
		}
	}

	pConsumer, err := c.consumer.ConsumePartition(topic, partition, int64(offset))
	if err != nil {
		return nil, errors.WithPrevious(err, fmt.Sprintf(`cannot initiate partition consumer for %s_%d`, topic, partition))
	}

	c.partitionConsumer = pConsumer

	go c.runBufferMetrics(pConsumer)

	go c.consumeErrors(pConsumer)

	go c.consumeRecords(pConsumer, partitionEnd)

	return c.consumerEvents, nil
}

func (c *partitionConsumer) Errors() <-chan *Error {
	return c.consumerErrors
}

func (c *partitionConsumer) Id() string {
	return c.id
}

func (c *partitionConsumer) consumeErrors(consumer sarama.PartitionConsumer) {
	for err := range consumer.Errors() {
		c.logger.Error(err)
		c.consumerErrors <- &Error{err}
	}
	close(c.consumerErrors)
}

func (c *partitionConsumer) runBufferMetrics(consumer sarama.PartitionConsumer) {

	ticker := time.NewTicker(1 * time.Second)
	for range ticker.C {
		c.metrics.consumerBuffer.Count(float64(len(consumer.Messages())), map[string]string{
			`topic`:     ``,
			`partition`: `0`,
			`type`:      `sarama`,
		})

		c.metrics.consumerBufferMax.Count(float64(cap(consumer.Messages())), map[string]string{
			`topic`:     ``,
			`partition`: `0`,
			`type`:      `sarama`,
		})

		c.metrics.consumerBuffer.Count(float64(len(c.consumerEvents)), map[string]string{
			`topic`:     ``,
			`partition`: `0`,
			`type`:      `k_stream`,
		})

		c.metrics.consumerBufferMax.Count(float64(cap(c.consumerEvents)), map[string]string{
			`topic`:     ``,
			`partition`: `0`,
			`type`:      `k_stream`,
		})
	}
}

func (c *partitionConsumer) consumeRecords(consumer sarama.PartitionConsumer, highWatermark int64) {

MainLoop:
	for {
		select {
		case msg, ok := <-consumer.Messages():
			if !ok {
				break MainLoop
			}

			latency := time.Since(msg.Timestamp).Nanoseconds() / 1e6

			c.metrics.endToEndLatency.Observe(float64(latency*1e3), map[string]string{
				`topic`:     msg.Topic,
				`partition`: fmt.Sprint(msg.Partition),
			})

			c.logger.Trace(fmt.Sprintf(`message [%d] received after %d miliseconds for %s[%d]`,
				msg.Offset, latency, msg.Topic, msg.Partition))

			// TODO remove this
			c.logger.Debug(`k-stream.Partition-consumer.Trace.Sync`,
				fmt.Sprintf(`message received for topic [%s], partition [%d] with key [%s] and value [%s] after %d milisconds delay at %s`,
					msg.Topic,
					msg.Partition,
					string(msg.Key),
					string(msg.Value),
					time.Since(msg.Timestamp).Nanoseconds()/1e6,
					time.Now(),
				))

			c.consumerEvents <- &data.Record{
				Key:       msg.Key,
				Value:     msg.Value,
				Offset:    msg.Offset,
				Topic:     msg.Topic,
				Partition: msg.Partition,
				Timestamp: msg.Timestamp,
				UUID:      uuid.New(),
				Headers:   msg.Headers,
			}

			//if highWatermark == 0 || highWatermark-1 == msg.Offset {
			if msg.Offset == highWatermark-1 {
				c.consumerEvents <- &PartitionEnd{
					tps: []TopicPartition{{
						Topic:     msg.Topic,
						Partition: msg.Partition,
					}},
				}
			}

		case <-c.closing:
			break MainLoop
		}
	}

	c.closed <- true
}

func (c *partitionConsumer) Close() error {

	c.logger.Info(fmt.Sprintf("[%s] closing... ", c.id))

	c.closing <- true
	<-c.closed

	if err := c.partitionConsumer.Close(); err != nil {

		if errs, ok := err.(sarama.ConsumerErrors); ok {
			for _, er := range errs {
				c.logger.Warn(fmt.Sprintf("partition consumer error while closing [%s] ", er))
			}
		}

		c.logger.Error(fmt.Sprintf("partition consumer close failed [%s] ", err))
	}

	if err := c.consumer.Close(); err != nil {
		c.logger.Error(fmt.Sprintf("consumer close failed [%s] ", err))
	}

	if err := c.offsets.Close(); err != nil {
		c.logger.Error(fmt.Sprintf("cannot close offsets [%s] ", err))
	}

	close(c.consumerEvents)
	c.cleanUpMetrics()
	c.logger.Info(fmt.Sprintf("[%s] closed", c.id))
	return nil
}

func (c *partitionConsumer) cleanUpMetrics() {
	c.metrics.consumerBuffer.UnRegister()
	c.metrics.consumerBufferMax.UnRegister()
	c.metrics.endToEndLatency.UnRegister()
}
