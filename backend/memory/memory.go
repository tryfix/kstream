/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package memory

import (
	"github.com/tryfix/kstream/backend"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
	"sync"
	"time"
)

type memoryRecord struct {
	key       []byte
	value     []byte
	createdAt time.Time
	expiry    time.Duration
}

type config struct {
	MetricsReporter metrics.Reporter
	Logger          log.Logger
}

func NewConfig() *config {
	conf := new(config)
	conf.parse()

	return conf
}

func (c *config) parse() {
	if c.Logger == nil {
		c.Logger = log.NewNoopLogger()
	}

	if c.MetricsReporter == nil {
		c.MetricsReporter = metrics.NoopReporter()
	}
}

type memory struct {
	records *sync.Map
	logger  log.Logger
	metrics struct {
		readLatency   metrics.Observer
		updateLatency metrics.Observer
		deleteLatency metrics.Observer
		storageSize   metrics.Gauge
	}
}

func Builder(config *config) backend.Builder {
	return func(name string) (backend backend.Backend, err error) {
		return NewMemoryBackend(config.Logger, config.MetricsReporter), nil
	}
}

func NewMemoryBackend(logger log.Logger, reporter metrics.Reporter) backend.Backend {

	m := &memory{
		logger:  logger,
		records: new(sync.Map),
	}

	labels := []string{`name`, `type`}
	m.metrics.readLatency = reporter.Observer(metrics.MetricConf{Path: `backend_read_latency_microseconds`, Labels: labels})
	m.metrics.updateLatency = reporter.Observer(metrics.MetricConf{Path: `backend_update_latency_microseconds`, Labels: labels})
	m.metrics.storageSize = reporter.Gauge(metrics.MetricConf{Path: `backend_storage_size`, Labels: labels})
	m.metrics.deleteLatency = reporter.Observer(metrics.MetricConf{Path: `backend_delete_latency_microseconds`, Labels: labels})

	go m.runCleaner()
	return m
}

func (m *memory) runCleaner() {
	ticker := time.NewTicker(1 * time.Millisecond)
	for range ticker.C {
		records := m.snapshot()
		for _, record := range records {
			if record.expiry > 0 && time.Since(record.createdAt).Nanoseconds() > record.expiry.Nanoseconds() {
				if err := m.Delete(record.key); err != nil {
					m.logger.Error(err)
				}
			}
		}
	}
}

func (m *memory) snapshot() []memoryRecord {
	records := make([]memoryRecord, 0)

	m.records.Range(func(key, value interface{}) bool {
		records = append(records, value.(memoryRecord))
		return true
	})

	return records
}

func (m *memory) Name() string {
	return `memory`
}

func (m *memory) String() string {
	return `memory`
}

func (m *memory) Persistent() bool {
	return false
}

func (m *memory) Set(key []byte, value []byte, expiry time.Duration) error {

	defer func(begin time.Time) {
		m.metrics.updateLatency.Observe(float64(time.Since(begin).Nanoseconds()/1e3), map[string]string{`name`: m.Name(), `type`: `memory`})
	}(time.Now())

	record := memoryRecord{
		key:       key,
		value:     value,
		expiry:    expiry,
		createdAt: time.Now(),
	}

	m.records.Store(string(key), record)

	return nil
}

func (m *memory) Get(key []byte) ([]byte, error) {

	defer func(begin time.Time) {
		m.metrics.readLatency.Observe(float64(time.Since(begin).Nanoseconds()/1e3), map[string]string{`name`: m.Name(), `type`: `memory`})
	}(time.Now())

	record, ok := m.records.Load(string(key))
	if !ok {
		return nil, nil
	}

	return record.(memoryRecord).value, nil
}

func (m *memory) RangeIterator(fromKy []byte, toKey []byte) backend.Iterator {
	panic("implement me")
}

func (m *memory) Iterator() backend.Iterator {
	records := m.snapshot()
	return &Iterator{
		records: records,
		valid:   len(records) > 0,
	}
}

func (m *memory) Delete(key []byte) error {

	defer func(begin time.Time) {
		m.metrics.deleteLatency.Observe(float64(time.Since(begin).Nanoseconds()/1e3), map[string]string{`name`: m.Name(), `type`: `memory`})
	}(time.Now())

	m.records.Delete(string(key))

	return nil
}

func (m *memory) Destroy() error { return nil }

func (m *memory) SetExpiry(time time.Duration) {}

func (m *memory) reportMetricsSize() {}

func (m *memory) Close() error {
	m.records = nil
	return nil
}

type Iterator struct {
	records    []memoryRecord
	currentKey int
	valid      bool
}

func (i *Iterator) SeekToFirst() {
	i.currentKey = 0
}

func (i *Iterator) SeekToLast() {
	i.currentKey = len(i.records) - 1
}

func (i *Iterator) Seek(key []byte) {
	for idx, r := range i.records {
		if string(r.key) == string(key) {
			i.currentKey = idx
		}
	}
}

func (i *Iterator) Next() {
	if i.currentKey == len(i.records)-1 {
		i.valid = false
		return
	}
	i.currentKey += 1
}

func (i *Iterator) Prev() {
	if i.currentKey < 0 {
		i.valid = false
		return
	}
	i.currentKey += 1
}

func (i *Iterator) Close() {
	i.records = nil
}

func (i *Iterator) Key() []byte {
	return i.records[i.currentKey].key
}

func (i *Iterator) Value() []byte {
	return i.records[i.currentKey].value
}

func (i *Iterator) Valid() bool {
	return i.valid
}

func (i *Iterator) Error() error {
	return nil
}
