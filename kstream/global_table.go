/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package kstream

import (
	"context"
	"github.com/tryfix/errors"
	"github.com/tryfix/kstream/data"
	"github.com/tryfix/kstream/kstream/encoding"
	"github.com/tryfix/kstream/kstream/processors"
	"github.com/tryfix/kstream/kstream/processors/join"
	"github.com/tryfix/kstream/kstream/store"
	"github.com/tryfix/log"
)

// Starting offset for the global table partition.
type GlobalTableOffset int64

// GlobalTableOffsetDefault defines the starting offset for the GlobalTable when GlobalTable stream syncing started.
const GlobalTableOffsetDefault GlobalTableOffset = 0

// GlobalTableOffsetLatest defines the beginning of the partition.
// Suitable for topics with retention policy delete since the topic can contains historical data.
const GlobalTableOffsetLatest GlobalTableOffset = -1

// globalTableStoreWriter overrides the persistence logic for GlobalTables.
var globalTableStoreWriter = func(r *data.Record, st store.Store) error {
	if _, ok := st.(store.IndexedStore); ok {
		return globalTableIndexedStoreWriter(r, st)
	}
	// tombstone handling
	if r.Value == nil {
		return st.Backend().Delete(r.Key)
	}

	return st.Backend().Set(r.Key, r.Value, 0)
}

var globalTableIndexedStoreWriter = func(r *data.Record, store store.Store) error {
	k, err := store.KeyEncoder().Decode(r.Key)
	if err != nil {
		return errors.WithPrevious(err, `indexable-store-writer key decode error`)
	}
	if r.Value == nil {
		return store.Delete(context.Background(), k)
	}

	v, err := store.ValEncoder().Decode(r.Value)
	if err != nil {
		return errors.WithPrevious(err, `indexable-store-writer value decode error`)
	}

	return store.Set(context.Background(), k, v, 0)
}

type globalTableOptions struct {
	initialOffset           GlobalTableOffset
	logger                  log.Logger
	backendWriter           StoreWriter
	recordVersionExtractor  RecordVersionExtractor
	recordVersionComparator RecordVersionComparator
}

type GlobalTableOption func(options *globalTableOptions)

// GlobalTableWithOffset overrides the default starting offset when GlobalTable syncing started.
func GlobalTableWithOffset(offset GlobalTableOffset) GlobalTableOption {
	return func(options *globalTableOptions) {
		options.initialOffset = offset
	}
}

// GlobalTableWithLogger overrides the default logger for the GlobalTable (default is NoopLogger).
func GlobalTableWithLogger(logger log.Logger) GlobalTableOption {
	return func(options *globalTableOptions) {
		options.logger = logger
	}
}

// GlobalTableWithVersionExtractor adds the version extractor for the GlobalTable from past records (default is nil).
func GlobalTableWithVersionExtractor(extractor RecordVersionExtractor) GlobalTableOption {
	return func(options *globalTableOptions) {
		options.recordVersionExtractor = extractor
	}
}

// GlobalTableWithVersionComparator adds the version extractor for the GlobalTable from past records (default is nil).
func GlobalTableWithVersionComparator(comparator RecordVersionComparator) GlobalTableOption {
	return func(options *globalTableOptions) {
		options.recordVersionComparator = comparator
	}
}

// GlobalTableWithBackendWriter overrides the persisting behavior of the GlobalTable.
// eg :
//	func(r *data.Record, store store.Store) error {
//		// tombstone handling
//		if r.Value == nil {
//			if err := store.Backend().Delete(r.Key); err != nil {
//				return err
//			}
//		}
//
//		return store.Backend().Set(r.Key, r.Value, 0)
//	}
func GlobalTableWithBackendWriter(writer StoreWriter) GlobalTableOption {
	return func(options *globalTableOptions) {
		options.backendWriter = writer
	}
}

type GlobalTable interface {
	Stream
}

type globalKTable struct {
	*kStream
	storeName string
	store     store.Store
	options   *globalTableOptions
}

func (t *globalKTable) To(topic string, keyEncoder encoding.Builder, valEncoder encoding.Builder, options ...SinkOption) {
	panic(`global table dose not support stream processing`)
}

func (t *globalKTable) Transform(transformer processors.TransFunc) Stream {
	panic(`global table dose not support stream transforming`)
}

func (t *globalKTable) Filter(filter processors.FilterFunc) Stream {
	panic(`global table dose not support stream processing`)
}

func (t *globalKTable) Process(processor processors.ProcessFunc) Stream {
	panic(`global table dose not support stream processing`)
}

func (t *globalKTable) Join(stream Stream, keyMapper join.KeyMapper, valMapper join.ValueMapper) Stream {
	panic(`global table to global table joins are not supported yet`)
}
