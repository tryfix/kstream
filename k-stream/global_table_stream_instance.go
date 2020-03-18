/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package kstream

import (
	"bytes"
	"context"
	"fmt"
	"github.com/olekukonko/tablewriter"

	"github.com/tryfix/errors"
	"github.com/tryfix/kstream/backend"
	"github.com/tryfix/kstream/consumer"
	"github.com/tryfix/kstream/data"
	"github.com/tryfix/kstream/k-stream/offsets"
	"github.com/tryfix/kstream/k-stream/store"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"

	"strconv"
	"time"
)

type tableInstance struct {
	tp                    tp
	offsetBackend         backend.Backend
	offsetKey             []byte
	store                 store.Store
	storeWriter           StoreWriter
	config                *globalKTable
	restartOnFailure      bool
	restartOnFailureCount int
	consumer              consumer.PartitionConsumer
	offsets               offsets.Manager
	synced, stopped       chan bool
	syncedCount           int64
	startOffset           int64
	endOffset             int64
	localOffset           int64
	logger                log.Logger
	metrics               struct {
		consumedLatency metrics.Observer
	}
}

func (t *tableInstance) Init() {

	t.synced = make(chan bool, 1)
	t.stopped = make(chan bool, 1)
	// by default broker Start offset is offset beginning
	t.localOffset = t.offsetLocal()

	// check storeName is recoverable or not
	if recoverableStore, ok := t.config.store.(store.RecoverableStore); ok {
		if err := recoverableStore.Recover(context.Background()); err != nil {
			t.logger.Fatal(fmt.Sprintf(`store recovery failed due to %s`, err))
		}
	}

	startOffset, err := t.offsets.GetOffsetOldest(t.tp.topic, t.tp.partition)
	if err != nil {
		t.logger.Fatal(fmt.Sprintf(`cannot read local offsetLocal due to %+v`, err))
	}

	endOffset, err := t.offsets.GetOffsetLatest(t.tp.topic, t.tp.partition)
	if err != nil {
		t.logger.Fatal(fmt.Sprintf(`cannot read local offsetLocal due to %+v`, err))
	}

	if t.config.options.initialOffset == GlobalTableOffsetLatest {
		t.startOffset = int64(GlobalTableOffsetLatest)
		t.endOffset = endOffset
		go t.Start()
		<-t.synced
		return
	}

	t.logger.Info(fmt.Sprintf(`brocker offsets found Start:%d and end:%d`, startOffset, endOffset))

	defer t.logger.Info(fmt.Sprintf(`table sync done with [%d] records`, t.syncedCount))

	// now get local offset from offset storeName
	// if local offset is > 0 and > beginOffset then reset beginOffset = local offset
	// non persistence backends dose not have a local offset
	if t.localOffset >= startOffset && t.localOffset < endOffset && t.store.Backend().Persistent() {
		startOffset = t.localOffset
		t.logger.Info(fmt.Sprintf(`offset %d found locally`, t.localOffset))
	}

	t.startOffset = startOffset
	t.endOffset = endOffset

	go t.Start()
	<-t.synced
}

func (t *tableInstance) Start() error {
	events, err := t.consumer.Consume(t.tp.topic, t.tp.partition, consumer.Offset(t.startOffset))
	if err != nil {
		//return errors.New( `cannot Start globalTableStream`, err)
		t.logger.Fatal(fmt.Sprintf(`cannot Start globalTableStream for due to %+v`, err))
	}

	t.logger.Info(fmt.Sprintf(`table syncing...`))

	ticker := time.NewTicker(1 * time.Second)
	go func(tic *time.Ticker, topic string, partition int32) {
		for range tic.C {
			t.logger.Info(fmt.Sprintf(`sync progress - [%d]%% done (%d/%d)`, t.syncedCount*100/t.endOffset, t.syncedCount, t.endOffset))
		}
	}(ticker, t.tp.topic, t.tp.partition)

	synced := false

	for event := range events {
		switch e := event.(type) {
		case *data.Record:

			t.metrics.consumedLatency.Observe(float64(time.Since(e.Timestamp).Nanoseconds()/1e3), map[string]string{
				`topic`:     e.Topic,
				`partition`: fmt.Sprint(e.Partition),
			})

			t.syncedCount++

			if err := t.process(e, t.restartOnFailureCount, nil); err != nil {
				t.logger.Error(
					fmt.Sprintf(`cannot process message due to %+v`, err))
				continue
			}

		case *consumer.PartitionEnd:
			if !synced {
				t.logger.Info(`partition ended`)
				ticker.Stop()
				synced = true
				t.synced <- true
			}

		case *consumer.Error:
			t.logger.Error(e)
		}
	}

	t.stopped <- true

	return nil
}

func (t *tableInstance) markOffset(offset int64) error {
	val := []byte(strconv.Itoa(int(offset)))
	return t.offsetBackend.Set(t.offsetKey, val, 0)
}

func (t *tableInstance) offsetLocal() int64 {
	if !t.store.Backend().Persistent() {
		return 0
	}

	key := []byte(fmt.Sprintf(`%s_%d`, t.tp.topic, t.tp.partition))
	byt, err := t.offsetBackend.Get(key)
	if err != nil {
		t.logger.Fatal(
			fmt.Sprintf(`cannot read local offsetLocal for %s[%d] - %+v`, t.tp.topic, t.tp.partition, err))
	}

	if len(byt) == 0 {
		t.logger.Warn(
			fmt.Sprintf(`offsetLocal dose not exist for %s[%d]`, t.tp.topic, t.tp.partition))
		if err := t.markOffset(0); err != nil {
			t.logger.Fatal(err)
		}
		return t.startOffset
	}

	offset, err := strconv.Atoi(string(byt))
	if err != nil {
		t.logger.Fatal(err)
	}

	t.logger.Info(
		fmt.Sprintf(`last synced offsetLocal for %s[%d] (%d)`, t.tp.topic, t.tp.partition, offset))

	return int64(offset)
}

func (t *tableInstance) process(r *data.Record, retry int, err error) error {
	if retry == 0 {
		return errors.WithPrevious(err, `cannot process message for []`)
	}

	if err := t.processRecord(r); err != nil {
		t.logger.Warn(
			fmt.Sprintf(`cannot process message on %s[%d] due to [%s], retring...`, r.Topic, r.Partition, err.Error()))
		return t.process(r, retry-1, err)
	}

	return nil
}

func (t *tableInstance) processRecord(r *data.Record) error {
	// log compaction (tombstone)
	if err := t.storeWriter(r, t.store); err != nil {
		return err
	}

	// if backend is non persistent no need to store the offset locally
	if !t.store.Backend().Persistent() {
		return nil
	}

	return t.markOffset(r.Offset)
}

func (t *tableInstance) print() {
	b := new(bytes.Buffer)
	table := tablewriter.NewWriter(b)
	table.SetHeader([]string{`GlobalTableConfig`, fmt.Sprint(t.tp.topic)})
	tableData := [][]string{
		{`partition`, fmt.Sprint(t.tp.partition)},
		{`offset.local`, fmt.Sprint(t.localOffset)},
		{`offset.broker.Start`, fmt.Sprint(t.startOffset)},
		{`offset.broker.end`, fmt.Sprint(t.endOffset)},
		{`synced`, fmt.Sprint(t.syncedCount)},
	}

	for _, v := range tableData {
		table.SetColumnAlignment([]int{tablewriter.ALIGN_LEFT})
		table.Append(v)
	}
	table.Render()
	t.logger.Info("\n" + b.String())
}
