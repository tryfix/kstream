/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package kstream

import (
	"fmt"
	"github.com/tryfix/errors"
	"github.com/tryfix/kstream/consumer"
	"github.com/tryfix/kstream/k-stream/changelog"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
	"sync"
)

type processorPool struct {
	id         string
	processors map[consumer.TopicPartition]*processor
	mu         *sync.Mutex
	topologies map[string]*kStream
	logger     log.Logger
	metrics    metrics.Reporter
	changelog  changelog.Builder
}

func newProcessorPool(id string, flows map[string]*kStream, changelog changelog.Builder, logger log.Logger, reporter metrics.Reporter) *processorPool {
	return &processorPool{
		id:         id,
		processors: make(map[consumer.TopicPartition]*processor),
		mu:         &sync.Mutex{},
		topologies: flows,
		logger:     logger,
		metrics:    reporter,
		changelog:  changelog,
	}
}

func (p *processorPool) Processor(tp consumer.TopicPartition) *processor {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.processors[tp]
}

func (p *processorPool) addProcessor(tp consumer.TopicPartition) error {

	processorId := fmt.Sprintf(`%s_%s_%d`, p.id, tp.Topic, tp.Partition)
	processor, err := newProcessor(processorId, tp, p.changelog, p.logger, p.metrics)
	if err != nil {
		return errors.WithPrevious(err, `cannot start stream processor`)
	}

	processor.topologyBuilder = p.topologies[tp.Topic].topology
	//processor.changelogEnabled = p.topologies[tp.Topic].changelog.enabled
	processor.taskPoolConfig = p.topologies[tp.Topic].config.workerPool
	p.processors[tp] = processor

	return nil
}

func (p *processorPool) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, processor := range p.processors {
		processor.Stop()
	}
}

func (p *processorPool) Remove(tp consumer.TopicPartition) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.processors[tp].Stop()
	logger.Info(
		fmt.Sprintf(`processor for %s stopped`, tp.String()))
}
