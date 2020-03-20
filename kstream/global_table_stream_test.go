package kstream

import (
	"fmt"
	"github.com/tryfix/kstream/admin"
	"github.com/tryfix/kstream/backend/memory"
	"github.com/tryfix/kstream/consumer"
	"github.com/tryfix/kstream/data"
	"github.com/tryfix/kstream/kstream/encoding"
	"github.com/tryfix/kstream/kstream/offsets"
	"github.com/tryfix/kstream/kstream/store"
	"github.com/tryfix/kstream/producer"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
	"sync"
	"testing"
	"time"
)

func TestGlobalTableStream_StartStreams(t *testing.T) {
	initStream := func(startOffset GlobalTableOffset) (*globalTableStream, func(expectedCount int), func(start int, end int)) {
		mocksTopics := admin.NewMockTopics()
		kafkaAdmin := &admin.MockKafkaAdmin{
			Topics: mocksTopics,
		}
		offsetManager := &offsets.MockManager{Topics: mocksTopics}

		topics := make(map[string]*admin.Topic)
		stores := make(map[string]store.Store)
		tables := make(map[string]*globalKTable)
		opts := new(globalTableOptions)
		opts.backendWriter = globalTableStoreWriter
		opts.initialOffset = startOffset

		for i := 0; i < 1; i++ {
			name := fmt.Sprintf(`topic%d`, i)
			topics[name] = &admin.Topic{
				Name:              name,
				NumPartitions:     100,
				ReplicationFactor: 1,
			}

			stor, _ := store.NewStore(name, encoding.StringEncoder{}, encoding.StringEncoder{}, store.WithBackend(memory.NewMemoryBackend(log.NewNoopLogger(), metrics.NoopReporter())))
			stores[name] = stor
			tables[name] = &globalKTable{store: stor, storeName: stor.Name(), options: opts}
		}

		if err := kafkaAdmin.CreateTopics(topics); err != nil {
			t.Error(err)
		}

		gTableStream, err := newGlobalTableStream(tables, &GlobalTableStreamConfig{
			ConsumerBuilder: consumer.NewMockPartitionConsumerBuilder(mocksTopics, offsetManager),
			BackendBuilder:  memory.Builder(memory.NewConfig()),
			OffsetManager:   offsetManager,
			KafkaAdmin:      kafkaAdmin,
			Metrics:         metrics.NoopReporter(),
			Logger:          log.NewNoopLogger(),
		})
		if err != nil {
			t.Error(err)
		}

		assertFunc := func(expectedCount int) {
			count := 0
			for _, str := range stores {
				i, _ := str.GetAll(nil)
				for i.Valid() {
					count++
					i.Next()
				}
			}

			if count != expectedCount*len(topics) {
				t.Error(fmt.Sprintf(`invalid count have [%d]`, count))
				t.Fail()
			}
		}

		p := producer.NewMockProducer(mocksTopics)

		producerFunc := func(start int, count int) {
			for i := start; i <= count; i++ {
				for topic := range topics {
					_, _, _ = p.Produce(nil, &data.Record{
						Key:   []byte(fmt.Sprint(i)),
						Value: []byte(`v`),
						Topic: topic,
					})
				}
			}
		}

		return gTableStream, assertFunc, producerFunc
	}

	t.Run(`NoMessage`, func(t *testing.T) {
		gTableStream, assertFunc, producerFunc := initStream(GlobalTableOffsetLatest)

		producerFunc(0, 0)

		wg := &sync.WaitGroup{}
		gTableStream.StartStreams(wg)

		go func() {
			time.Sleep(1 * time.Second)
			gTableStream.stop()
		}()
		wg.Wait()

		assertFunc(0)
	})

	t.Run(`Latest`, func(t *testing.T) {
		gTableStream, assertFunc, producerFunc := initStream(GlobalTableOffsetLatest)

		producerFunc(0, 3333)

		wg := &sync.WaitGroup{}
		gTableStream.StartStreams(wg)

		go func() {
			time.Sleep(1 * time.Second)
			gTableStream.stop()
		}()
		wg.Wait()

		assertFunc(0)
	})

	t.Run(`Oldest`, func(t *testing.T) {
		gTableStream, assertFunc, producerFunc := initStream(GlobalTableOffsetDefault)

		producerFunc(0, 3332)

		wg := &sync.WaitGroup{}
		gTableStream.StartStreams(wg)

		go func() {
			time.Sleep(1 * time.Second)
			gTableStream.stop()
		}()
		wg.Wait()

		assertFunc(3333)
	})

	t.Run(`OldestAfterStarted`, func(t *testing.T) {
		gTableStream, assertFunc, producerFunc := initStream(GlobalTableOffsetDefault)

		producerFunc(0, 3332)

		wg := &sync.WaitGroup{}
		gTableStream.StartStreams(wg)

		time.Sleep(1 * time.Second)

		producerFunc(3333, 6665)

		go func() {
			time.Sleep(1 * time.Second)
			gTableStream.stop()
		}()
		wg.Wait()

		assertFunc(6666)
	})

	t.Run(`LatestAfterStarted`, func(t *testing.T) {
		gTableStream, assertFunc, producerFunc := initStream(GlobalTableOffsetLatest)

		producerFunc(0, 3332)

		wg := &sync.WaitGroup{}
		gTableStream.StartStreams(wg)

		time.Sleep(1 * time.Second)

		producerFunc(3334, 6666)

		go func() {
			time.Sleep(1 * time.Second)
			gTableStream.stop()
		}()
		wg.Wait()

		assertFunc(3333)
	})
}
