/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package changelog

import (
	"context"
	"github.com/tryfix/kstream/data"
	"github.com/tryfix/kstream/producer"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
	"sync"
	"time"
)

// Buffer holds a temporary changelog Buffer
type Buffer struct {
	records       []*data.Record
	mu            *sync.Mutex
	shouldFlush   chan bool
	flushInterval time.Duration
	bufferSize    int
	logger        log.Logger
	producer      producer.Producer
	lastFlushed   time.Time
	metrics       struct {
		flushLatency metrics.Observer
	}
}

// NewBuffer creates a new Buffer object
func NewBuffer(p producer.Producer, size int, flushInterval time.Duration, logger log.Logger) *Buffer {
	flush := 1 * time.Second
	if flushInterval != 0 {
		flush = flushInterval
	}

	b := &Buffer{
		records:       make([]*data.Record, 0, size),
		mu:            new(sync.Mutex),
		producer:      p,
		bufferSize:    size,
		logger:        logger,
		shouldFlush:   make(chan bool, 1),
		flushInterval: flush,
		lastFlushed:   time.Now(),
	}

	go b.runFlusher()

	return b
}

// Clear clears the Buffer
func (b *Buffer) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.flushAll(); err != nil {
		b.logger.ErrorContext(context.Background(), `k-stream.changelog.buffer`, err)
	}

}

func (b *Buffer) Records() []*data.Record {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.records
}

// Store stores the record in Buffer
func (b *Buffer) Store(record *data.Record) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.records = append(b.records, record)

	if len(b.records) >= b.bufferSize {
		b.flush()
	}
}

func (b *Buffer) runFlusher() {
	//tic := time.NewTicker(b.flushInterval)
	//defer tic.Stop()
	//
	//for range tic.C {
	//
	//	if time.Since(b.lastFlushed) <= b.flushInterval {
	//		continue
	//	}
	//
	//	b.mu.Lock()
	//	if len(b.records) > 0 {
	//		b.flush()
	//	}
	//	b.mu.Unlock()
	//
	//}
}

func (b *Buffer) flush() {
	if err := b.flushAll(); err != nil {
		b.logger.ErrorContext(context.Background(), `k-stream.changelog.buffer`, err)
	}

	b.logger.Trace(`k-stream.changelog.buffer`, `buffer flushed`)
}

func (b *Buffer) flushAll() error {
	begin := time.Now()
	defer func(t time.Time) {
		b.metrics.flushLatency.Observe(float64(time.Since(begin).Nanoseconds()/1e3), nil)
	}(begin)

	// publish buffer to kafka and clear on success
	//deDuplicated := deDuplicate(b.records)
	//if len(deDuplicated) > 0 {
	//	if err := b.producer.ProduceBatch(context.Background(), deDuplicated); err != nil {
	//		return err
	//	}
	//}

	if err := b.producer.ProduceBatch(context.Background(), b.records); err != nil {
		return err
	}

	b.reset()

	return nil
}

func (b *Buffer) Delete(record *data.Record) {
	record.Value = nil
	b.Store(record)
}

func (b *Buffer) reset() {
	b.records = make([]*data.Record, 0, b.bufferSize)
	b.lastFlushed = time.Now()
}

func (b *Buffer) Close() {
	// flush existing buffer
	b.logger.Info(`k-stream.changelog.buffer`, `flushing buffer...`)
	if err := b.flushAll(); err != nil {
		b.logger.ErrorContext(context.Background(), `k-stream.changelog.buffer`, err)
	}
}
