/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package producer

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/tryfix/errors"
	"github.com/tryfix/kstream/data"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
	"time"
)

type Builder func(configs *Config) (Producer, error)

type RequiredAcks int

const (
	// NoResponse doesn't send any response, the TCP ACK is all you get.
	NoResponse RequiredAcks = 0

	// WaitForLeader waits for only the local commit to succeed before responding.
	WaitForLeader RequiredAcks = 1

	// WaitForAll waits for all in-sync replicas to commit before responding.
	// The minimum number of in-sync replicas is configured on the broker via
	// the `min.insync.replicas` configuration key.
	WaitForAll RequiredAcks = -1
)

func (ack RequiredAcks) String() string {
	a := `NoResponse`

	if ack == WaitForLeader {
		a = `WaitForLeader`
	}

	if ack == WaitForAll {
		a = `WaitForAll`
	}

	return a
}

type Partitioner int

const (
	HashBased Partitioner = iota
	Manual
	Random
)

type Producer interface {
	Produce(ctx context.Context, message *data.Record) (partition int32, offset int64, err error)
	ProduceBatch(ctx context.Context, messages []*data.Record) error
	Close() error
}

type saramaProducer struct {
	id             string
	config         *Config
	saramaProducer sarama.SyncProducer
	logger         log.Logger
	metrics        *metricsReporter
}

type metricsReporter struct {
	produceLatency      metrics.Observer
	batchProduceLatency metrics.Observer
}

func NewProducer(configs *Config) (Producer, error) {
	if err := configs.validate(); err != nil {
		return nil, err
	}

	configs.Logger.Info(`saramaProducer [` + configs.Id + `] initiating...`)
	prd, err := sarama.NewSyncProducer(configs.BootstrapServers, configs.Config)
	if err != nil {
		return nil, errors.WithPrevious(err, fmt.Sprintf(`[%s] init failed`, configs.Id))
	}

	defer configs.Logger.Info(`saramaProducer [` + configs.Id + `] initiated`)

	labels := []string{`topic`, `partition`}
	return &saramaProducer{
		id:             configs.Id,
		config:         configs,
		saramaProducer: prd,
		logger:         configs.Logger,
		metrics: &metricsReporter{
			produceLatency: configs.MetricsReporter.Observer(metrics.MetricConf{
				Path:        `k_stream_producer_produced_latency_microseconds`,
				Labels:      labels,
				ConstLabels: map[string]string{`producer_id`: configs.Id},
			}),
			batchProduceLatency: configs.MetricsReporter.Observer(metrics.MetricConf{
				Path:        `k_stream_producer_batch_produced_latency_microseconds`,
				Labels:      append(labels, `size`),
				ConstLabels: map[string]string{`producer_id`: configs.Id},
			}),
		},
	}, nil
}

func (p *saramaProducer) Close() error {
	defer p.logger.Info(fmt.Sprintf(`saramaProducer [%s] closed`, p.id))
	return p.saramaProducer.Close()
}

func (p *saramaProducer) Produce(ctx context.Context, message *data.Record) (partition int32, offset int64, err error) {
	t := time.Now()

	m := &sarama.ProducerMessage{
		Topic:     message.Topic,
		Key:       sarama.ByteEncoder(message.Key),
		Value:     sarama.ByteEncoder(message.Value),
		Timestamp: t,
	}

	for _, header := range message.Headers.All() {
		m.Headers = append(m.Headers, *header)
	}

	if !message.Timestamp.IsZero() {
		m.Timestamp = message.Timestamp
	}

	if message.Partition > 0 {
		m.Partition = message.Partition
	}

	pr, o, err := p.saramaProducer.SendMessage(m)
	if err != nil {
		return 0, 0, errors.WithPrevious(err, `cannot send message`)
	}

	p.metrics.produceLatency.Observe(float64(time.Since(t).Nanoseconds()/1e3), map[string]string{
		`topic`:     message.Topic,
		`partition`: fmt.Sprint(pr),
	})

	p.logger.TraceContext(ctx, fmt.Sprintf("Delivered message to topic %s [%d] at offset %d",
		message.Topic, pr, o))

	return pr, o, nil
}

func (p *saramaProducer) ProduceBatch(ctx context.Context, messages []*data.Record) error {
	t := time.Now()
	saramaMessages := make([]*sarama.ProducerMessage, 0, len(messages))
	for _, message := range messages {

		m := &sarama.ProducerMessage{
			Topic:     message.Topic,
			Key:       sarama.ByteEncoder(message.Key),
			Value:     sarama.ByteEncoder(message.Value),
			Timestamp: t,
		}

		for _, header := range message.Headers {
			m.Headers = append(m.Headers, *header)
		}

		if !message.Timestamp.IsZero() {
			m.Timestamp = message.Timestamp
		}

		if message.Partition > 0 {
			m.Partition = message.Partition
		}

		saramaMessages = append(saramaMessages, m)
	}

	err := p.saramaProducer.SendMessages(saramaMessages)
	if err != nil {
		return errors.WithPrevious(err, `cannot produce batch`)
	}

	partition := fmt.Sprint(messages[0].Partition)
	p.metrics.batchProduceLatency.Observe(float64(time.Since(t).Nanoseconds()/1e3), map[string]string{
		`topic`:     messages[0].Topic,
		`partition`: partition,
		`size`:      fmt.Sprint(len(messages)),
	})
	p.logger.TraceContext(ctx, fmt.Sprintf("message bulk delivered %s[%s]", messages[0].Topic, partition))
	return nil
}
