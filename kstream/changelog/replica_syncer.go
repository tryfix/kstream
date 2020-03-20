package changelog

import (
	"fmt"
	"github.com/tryfix/errors"
	"github.com/tryfix/kstream/consumer"
	"github.com/tryfix/kstream/data"
	"github.com/tryfix/log"
)

type replicaSyncer struct {
	cache    *cache
	tp       consumer.TopicPartition
	consumer consumer.PartitionConsumer
	syncing  bool
	logger   log.Logger
	running  bool
}

func (rs *replicaSyncer) Sync(startOffset int64) (started chan bool, syncErrors chan error) {
	started = make(chan bool)
	syncErrors = make(chan error)

	go rs.initSync(startOffset, started, syncErrors)

	return started, syncErrors
}

func (rs *replicaSyncer) initSync(startOffset int64, started chan bool, syncErrors chan error) {
	if startOffset == 0 {
		startOffset = int64(consumer.Earliest)
	}

	events, err := rs.consumer.Consume(rs.tp.Topic, rs.tp.Partition, consumer.Offset(startOffset))
	if err != nil {
		syncErrors <- errors.WithPrevious(err, fmt.Sprintf(`cannot read partition %s[%d]`,
			rs.tp.Topic, rs.tp.Partition))
		return
	}

	rs.logger.Info(fmt.Sprintf(`partition consumer started at offset [%d]`, startOffset))

	started <- true
	rs.syncing = true

	for event := range events {
		switch ev := event.(type) {
		case *data.Record:
			if err := rs.cache.Put(ev); err != nil {
				syncErrors <- errors.WithPrevious(err, `writing to cache failed`)
			}
		case *consumer.PartitionEnd:
			rs.logger.Info(fmt.Sprintf(`replica sync completed for [%s]`, rs.tp))
		case *consumer.Error:
			rs.logger.Error(err)
		}
	}

	close(started)
	close(syncErrors)
	rs.syncing = false
	rs.running = true
}

func (rs *replicaSyncer) Stop() error {
	if rs.running {
		rs.logger.Info(`sync not running`)
		return nil
	}

	return rs.consumer.Close()
}
