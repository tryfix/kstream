/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package rocks
//
//import (
//	"fmt"
//	"github.com/tryfix/errors"
//	"github.com/tryfix/kstream/backend"
//	"github.com/tryfix/log"
//	"github.com/tryfix/metrics"
//	"github.com/tecbot/gorocksdb"
//	"os"
//	"path/filepath"
//	"time"
//)
//
//type db struct {
//	db      *gorocksdb.DB
//	name    string
//	expiry  time.Duration
//	logger  log.PrefixedLogger
//	metrics struct {
//		ticker        *time.Ticker
//		readLatency   metrics.Observer
//		updateLatency metrics.Observer
//		deleteLatency metrics.Observer
//		storageSize   metrics.Gauge
//	}
//}
//
//type config struct {
//	Dir             string
//	SyncCommit      bool
//	expiry          time.Duration
//	Logger          log.PrefixedLogger
//	MetricsReporter metrics.Reporter
//}
//
//func NewConfig() *config {
//	config := new(config)
//	config.parse()
//
//	return config
//}
//
//func (c *config) parse() {
//	if c.Dir == `` {
//		c.Dir = `storage`
//	}
//
//	if c.MetricsReporter == nil {
//		c.MetricsReporter = metrics.NoopReporter()
//	}
//
//	if c.Logger == nil {
//		c.Logger = log.Constructor.PrefixedLog()
//	}
//}
//
//func Builder(config *config) backend.Builder {
//
//	config.parse()
//
//	return func(name string) (backend backend.Backend, e error) {
//		return NewRocksDb(name, config)
//	}
//}
//
//func NewRocksDb(name string, config *config) (backend.Backend, error) {
//
//	conf := gorocksdb.NewDefaultOptions()
//	conf.SetCreateIfMissing(true)
//	conf.SetUseFsync(config.SyncCommit)
//
//	if config.expiry > 0 {
//		conf.SetWALTtlSeconds(uint64(config.expiry.Round(time.Second)))
//	}
//
//	if err := createDir(config.Dir); err != nil {
//		return nil, errors.WithPrevious(err, `cannot create dir`)
//	}
//
//	l, err := gorocksdb.OpenDb(conf, config.Dir+`/`+name)
//	if err != nil {
//		config.Logger.Fatal(`k-stream.backend.Rocksdb`, fmt.Sprintf(`cannt open rocks db instance - %+v`, err))
//	}
//
//	r := &db{
//		db:     l,
//		name:   name,
//		logger: config.Logger,
//	}
//
//	labels := []string{`name`, `type`}
//
//	r.metrics.readLatency = config.MetricsReporter.Observer(metrics.MetricConf{Path: `store_backend_read_latency_microseconds`, Labels: labels})
//	r.metrics.updateLatency = config.MetricsReporter.Observer(metrics.MetricConf{Path: `store_backend_update_latency_microseconds`, Labels: labels})
//	r.metrics.storageSize = config.MetricsReporter.Gauge(metrics.MetricConf{Path: `store_backend_storage_size`, Labels: labels})
//	r.metrics.deleteLatency = config.MetricsReporter.Observer(metrics.MetricConf{Path: `store_backend_delete_latency_microseconds`, Labels: labels})
//	r.metrics.ticker = time.NewTicker(10 * time.Second)
//
//	go r.reportMetricsSize(config.Dir + `/` + name)
//
//	return r, err
//
//}
//
//func createDir(path string) error {
//	return os.MkdirAll(path, os.ModePerm)
//}
//
//func (r *db) Name() string {
//	return `rocks`
//}
//
//func (r *db) String() string {
//	return `rocks`
//}
//
//func (r *db) Persistent() bool {
//	return true
//}
//
//func (r *db) Set(key []byte, value []byte, expiry time.Duration) error {
//	defer func(begin time.Time) {
//		r.metrics.updateLatency.Observe(float64(time.Since(begin).Nanoseconds()/1e3), map[string]string{`name`: r.db.Name(), `type`: `rocks`})
//	}(time.Now())
//
//	opts := gorocksdb.NewDefaultWriteOptions()
//	defer opts.Destroy()
//
//	err := r.db.Put(opts, key, value)
//	if err == nil {
//		// TODO remove this
//		r.logger.Debug(`k-stream.StoreBackend.Rocks.Trace.Sync`,
//			fmt.Sprintf(`record synced with key [%s] and value [%s] at %s`,
//				string(key),
//				string(value),
//				time.Now(),
//			))
//	} else {
//		// TODO remove this
//		r.logger.Error(`k-stream.StoreBackend.Rocks.Trace.Sync`,
//			fmt.Sprintf(`record synced failed with key [%s] and value [%s] at %s`,
//				string(key),
//				string(value),
//				time.Now(),
//			))
//	}
//
//	return err
//}
//
//func (r *db) Get(key []byte) ([]byte, error) {
//
//	defer func(begin time.Time) {
//		r.metrics.readLatency.Observe(float64(time.Since(begin).Nanoseconds()/1e3), map[string]string{`name`: r.db.Name(), `type`: `rocks`})
//	}(time.Now())
//
//	opts := gorocksdb.NewDefaultReadOptions()
//	defer opts.Destroy()
//
//	return r.db.GetBytes(opts, key)
//
//}
//
//func (r *db) RangeIterator(fromKy []byte, toKey []byte) backend.Iterator {
//	rOpts := gorocksdb.NewDefaultReadOptions()
//	rOpts.SetIterateUpperBound(fromKy)
//
//	return &iterator{
//		itr: r.db.NewIterator(rOpts),
//	}
//}
//
//func (r *db) Iterator() backend.Iterator {
//	rOpts := gorocksdb.NewDefaultReadOptions()
//
//	return &iterator{
//		itr: r.db.NewIterator(rOpts),
//	}
//}
//
//func (r *db) Delete(key []byte) error {
//
//	defer func(begin time.Time) {
//		r.metrics.deleteLatency.Observe(float64(time.Since(begin).Nanoseconds()/1e3), map[string]string{`name`: r.db.Name(), `type`: `rocks`})
//	}(time.Now())
//
//	opts := gorocksdb.NewDefaultWriteOptions()
//	defer opts.Destroy()
//
//	err := r.db.Delete(opts, key)
//	if err == nil {
//		// TODO remove this
//		r.logger.Debug(`k-stream.PartitionConsumer.Trace.Sync`,
//			fmt.Sprintf(`record delete with key [%s] at %s`,
//				string(key),
//				time.Now(),
//			))
//	} else {
//		// TODO remove this
//		r.logger.Error(`k-stream.PartitionConsumer.Trace.Sync`,
//			fmt.Sprintf(`record delete failed with key [%s] at %s`,
//				string(key),
//				time.Now(),
//			))
//	}
//
//	return err
//}
//
//func (r *db) Destroy() error {
//	opts := gorocksdb.NewDefaultOptions()
//	return gorocksdb.DestroyDb(r.db.Name(), opts)
//}
//
//func (r *db) SetExpiry(duration time.Duration) {
//	r.expiry = duration
//}
//
//func (r *db) Close() error {
//	// TODO before close the db remaining data on the memory must be flushed
//	//flushOpts := gorocksdb.NewDefaultFlushOptions()
//	//flushOpts.SetWait(true)
//	//if err := r.db.Flush(flushOpts); err != nil {
//	//	return err
//	//}
//	//flushOpts.Destroy()
//	//r.db.Close()
//	r.metrics.ticker.Stop()
//	return nil
//}
//
//func (r *db) reportMetricsSize(path string) {
//
//	for range r.metrics.ticker.C {
//		var size int64
//		if err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
//			if err != nil {
//				return err
//			}
//			size += info.Size()
//			return nil
//		}); err != nil {
//			r.logger.Error(`k-stream.backend.Rocksdb`, err)
//			continue
//		}
//		r.metrics.storageSize.Count(float64(size), map[string]string{`name`: r.db.Name(), `type`: `rocks`})
//	}
//
//}
//
//type iterator struct {
//	itr *gorocksdb.Iterator
//}
//
//func (i *iterator) SeekToFirst() {
//	i.itr.SeekToFirst()
//}
//
//func (i *iterator) SeekToLast() {
//	i.itr.SeekToLast()
//}
//
//func (i *iterator) Seek(key []byte) {
//	i.itr.Seek(key)
//}
//
//func (i *iterator) Next() {
//	i.itr.Next()
//}
//
//func (i *iterator) Prev() {
//	i.itr.Prev()
//}
//
//func (i *iterator) Close() {
//	i.itr.Close()
//}
//
//func (i *iterator) Key() []byte {
//	return i.itr.Key().Data()
//}
//
//func (i *iterator) Value() []byte {
//	return i.itr.Value().Data()
//}
//
//func (i *iterator) Valid() bool {
//	return i.itr.Valid()
//}
//
//func (i *iterator) Error() error {
//	return i.itr.Err()
//}
