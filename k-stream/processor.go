/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package kstream

import (
	"context"
	"fmt"
	"github.com/tryfix/errors"
	"github.com/tryfix/kstream/consumer"
	"github.com/tryfix/kstream/data"
	"github.com/tryfix/kstream/k-stream/changelog"
	kContext "github.com/tryfix/kstream/k-stream/context"
	"github.com/tryfix/kstream/k-stream/topology"
	"github.com/tryfix/kstream/k-stream/worker_pool"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
	"github.com/tryfix/traceable-context"
	"time"
)

type processor struct {
	id               string
	topicPartition   consumer.TopicPartition
	topologyBuilder  *topology.TopologyBuilder
	changelogEnabled bool
	changelog        changelog.Changelog
	changelogBuilder changelog.Builder
	records          <-chan *data.Record
	context          context.Context
	cancel           context.CancelFunc
	changelogMarks   chan *data.Record
	taskPoolConfig   *worker_pool.PoolConfig
	taskPool         *worker_pool.Pool
	logger           log.Logger
	metricsReporter  metrics.Reporter
	metrics          struct {
		processedLatency metrics.Observer
	}
}

func newProcessor(id string, tp consumer.TopicPartition, changelog changelog.Builder, logger log.Logger, metricsReporter metrics.Reporter) (*processor, error) {

	ctx, cancelFunc := context.WithCancel(context.Background())

	p := &processor{
		id:               id,
		topicPartition:   tp,
		context:          ctx,
		cancel:           cancelFunc,
		changelogMarks:   make(chan *data.Record),
		logger:           logger.NewLog(log.Prefixed(fmt.Sprintf(`processor-%s-%d`, tp.Topic, tp.Partition))),
		changelogBuilder: changelog,
		metricsReporter:  metricsReporter,
	}

	p.metrics.processedLatency = metricsReporter.Observer(metrics.MetricConf{
		Path:   `k_stream_stream_processor_job_processed_latency_microseconds`,
		Labels: []string{`topic`, `partition`},
	})

	return p, nil
}

func (p *processor) boot() error {
	defer p.logger.Info(`processor booted`)

	p.taskPool = worker_pool.NewPool(
		p.topicPartition.String(),
		p.topologyBuilder,
		p.metricsReporter,
		p.logger,
		p.taskPoolConfig)

	if p.changelogEnabled {

		stateChangelog, err := p.changelogBuilder(p.id, p.topicPartition.Topic, p.topicPartition.Partition)
		if err != nil {
			return errors.WithPrevious(err, `cannot init changelog`)
		}

		p.changelog = stateChangelog

		records, err := p.changelog.ReadAll(p.context)
		if err != nil {
			return errors.WithPrevious(err, `cannot recover`)
		}

		for _, record := range records {
			ctx := p.createContext(record)
			// these records are already marked in the changelog so START execution immediately
			p.execute(ctx, record.Timestamp, record)
		}
	}

	return nil
}

func (p *processor) start() {

	p.logger.Info("processor started")

	for record := range p.records {
		p.process(record)
	}

	// records chan is closed stop the processor
	p.Stop()

}

func (p *processor) process(record *data.Record) {

	ctx := p.createContext(record)
	// if message processing method is sync changelog marking is not necessary
	begin := time.Now()
	if p.taskPoolConfig.Order == worker_pool.OrderPreserved {
		p.taskPool.Run(ctx, record.Key, record.Value, func() {
			p.metrics.processedLatency.Observe(float64(time.Since(begin).Nanoseconds()/1e3), map[string]string{
				`topic`:     p.topicPartition.Topic,
				`partition`: fmt.Sprint(p.topicPartition.Partition),
			})
			p.changelogMarks <- record
		})
		return
	}

	//first mark record in the changelog
	if p.changelogEnabled {
		if err := p.changelog.Put(ctx, record); err != nil {
			p.logger.ErrorContext(ctx, fmt.Sprintf(`cannot save to changelog - %+v`, err))
		} else {
			// send record to marked chen
			p.changelogMarks <- record
			p.logger.TraceContext(ctx, "record mark on changelog")
		}
	}

	p.execute(ctx, begin, record)
}

func (p *processor) execute(ctx context.Context, begin time.Time, record *data.Record) {

	p.taskPool.Run(ctx, record.Key, record.Value, func() {
		p.commit(ctx, begin, record)
	})

}

func (p *processor) commit(ctx context.Context, begin time.Time, record *data.Record) {

	// processing is done delete from changelog
	if p.changelogEnabled {
		if err := p.changelog.Delete(ctx, record); err != nil {
			p.logger.ErrorContext(ctx, fmt.Sprintf(`cannot delete from changelog due to %+v`, err))
			return
		}

		p.logger.TraceContext(ctx, `record deleted from changelog`)
	}

	p.metrics.processedLatency.Observe(float64(time.Since(begin).Nanoseconds()/1e3), map[string]string{
		`topic`:     p.topicPartition.Topic,
		`partition`: fmt.Sprint(p.topicPartition.Partition),
	})
}

func (p *processor) createContext(record *data.Record) context.Context {
	return kContext.FromRecord(traceable_context.WithUUID(record.UUID), record)
}

func (p *processor) Stop() {

	p.logger.Info(`processor stopping...`)

	//p.taskPool.Stop()

	defer p.logger.Info(`processor stopped`)

	p.cancel()

}
