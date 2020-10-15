package main

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/tryfix/kstream/admin"
	"github.com/tryfix/kstream/consumer"
	"github.com/tryfix/kstream/data"
	"github.com/tryfix/kstream/examples/example_2/domain"
	"github.com/tryfix/kstream/examples/example_2/encoders"
	"github.com/tryfix/kstream/examples/example_2/events"
	"github.com/tryfix/kstream/examples/example_2/stream"
	"github.com/tryfix/kstream/kstream"
	"github.com/tryfix/kstream/kstream/offsets"
	"github.com/tryfix/kstream/kstream/worker_pool"
	"github.com/tryfix/kstream/producer"
	"github.com/tryfix/log"
	"os"
	"os/signal"
	"time"
)

func setupMockBuilders() *kstream.StreamBuilder {
	config := kstream.NewStreamBuilderConfig()
	topics := admin.NewMockTopics()
	kafkaAdmin := &admin.MockKafkaAdmin{
		Topics: topics,
	}

	if err := kafkaAdmin.CreateTopics(map[string]*admin.Topic{
		domain.ABCTopic: {
			Name:              domain.ABCTopic,
			NumPartitions:     2,
			ReplicationFactor: 1,
		},
	}); err != nil {
		log.Fatal(err)
	}

	prod := producer.NewMockProducer(topics)

	offsetManager := &offsets.MockManager{Topics: topics}
	go produceAAndB(prod)
	//go produceAccountDebited(prod)
	//go consumeMessageAndPrint(topics)

	config.BootstrapServers = []string{`localhost:9092`}
	config.ApplicationId = `k_stream_example_1`
	config.ConsumerCount = 1
	config.Host = `localhost:8100`
	config.AsyncProcessing = true
	//config.Store.StorageDir = `storage`
	config.Store.Http.Enabled = true
	config.Store.Http.Host = `:9002`
	config.ChangeLog.Enabled = false
	//config.ChangeLog.Buffer.Enabled = true
	//config.ChangeLog.Buffer.Size = 100
	//config.ChangeLog.ReplicationFactor = 3
	//config.ChangeLog.MinInSycReplicas = 2

	config.WorkerPool.Order = worker_pool.OrderByKey
	config.WorkerPool.NumOfWorkers = 100
	config.WorkerPool.WorkerBufferSize = 10

	config.Logger = log.NewLog(
		log.WithLevel(`INFO`),
		log.WithColors(true),
	).Log()

	return kstream.NewStreamBuilder(config,
		kstream.WithPartitionConsumerBuilder(consumer.NewMockPartitionConsumerBuilder(topics, offsetManager)),
		kstream.WithConsumerBuilder(consumer.NewMockConsumerBuilder(topics)),
		kstream.WithOffsetManager(offsetManager),
		kstream.WithKafkaAdmin(kafkaAdmin),
		kstream.WithProducerBuilder(func(configs *producer.Config) (i producer.Producer, e error) {
			return prod, nil
		}),
	)
}

func main() {
	builder := setupMockBuilders()

	//mockBackend := backend.NewMockBackend(`mock_backend`, time.Duration(time.Second * 3600))
	//accountDetailMockStore := store.NewMockStore(`account_detail_store`, encoders.KeyEncoder(), encoders.AccountDetailsUpdatedEncoder(), mockBackend)
	//builder.StoreRegistry().Register(accountDetailMockStore)
	//
	//customerProfileMockStore := store.NewMockStore(`customer_profile_store`, encoders.KeyEncoder(), encoders.CustomerProfileUpdatedEncoder(), mockBackend)
	//builder.StoreRegistry().Register(customerProfileMockStore)

	//builder.StoreRegistry().New(
	//	`account_detail_store`,
	//	encoders.KeyEncoder,
	//	encoders.AccountDetailsUpdatedEncoder)
	//
	//builder.StoreRegistry().New(
	//	`customer_profile_store`,
	//	encoders.KeyEncoder,
	//	encoders.CustomerProfileUpdatedEncoder)

	err := builder.Build(stream.InitStreams(builder)...)
	if err != nil {
		log.Fatal(`mock build failed`)
	}

	synced := make(chan bool, 1)

	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	streams := kstream.NewStreams(builder, kstream.NotifyOnStart(synced))
	go func() {
		select {
		case <-signals:
			streams.Stop()
		}
	}()

	if err := streams.Start(); err != nil {
		log.Fatal(log.WithPrefix(`boot.boot.Init`, `error in stream starting`), err)
	}

	//produceRealData()
}

//func produceAccountCredited(streamProducer producer.Producer) {
//
//	for {
//		key := rand.Int63n(100)
//		event := events.AccountCredited{
//			ID:        uuid.New().String(),
//			Type:      `account_credited`,
//			Timestamp: time.Now().UnixNano() / 1e6,
//		}
//		event.Body.AccountNo = key
//		event.Body.TransactionId = rand.Int63n(10000)
//		event.Body.Amount = 1000.00
//		event.Body.Reason = `utility bill transfer`
//		event.Body.DebitedFrom = 1111
//		event.Body.CreditedAt = time.Now().UnixNano() / 1e6
//		event.Body.Location = `Main Branch, City A`
//
//		encodedKey, err := encoders.KeyEncoder().Encode(key)
//		if err != nil {
//			log.Error(err, event)
//		}
//		encodedVal, err := encoders.AccountCreditedEncoder().Encode(event)
//		if err != nil {
//			log.Error(err, event)
//		}
//
//		_, _, err = streamProducer.Produce(context.Background(), &data.Record{
//			Key:       encodedKey,
//			Value:     encodedVal,
//			Topic:     `transaction`,
//			Timestamp: time.Now(),
//		})
//
//		if err != nil {
//			log.Error(err)
//		}
//
//		time.Sleep(time.Millisecond * 500)
//	}
//
//}

func produceAAndB(streamProducer producer.Producer) {

	for {
		key := uuid.New().String()
		produceA(streamProducer, key)
		time.Sleep(time.Millisecond * 100)
		produceB(streamProducer, key)
		time.Sleep(time.Millisecond * 100)
		produceC(streamProducer, key)

		time.Sleep(time.Millisecond * 500)

		key = uuid.New().String()
		produceB(streamProducer, key)
		time.Sleep(time.Millisecond * 100)
		produceC(streamProducer, key)
		time.Sleep(time.Millisecond * 100)
		produceA(streamProducer, key)

		time.Sleep(time.Millisecond * 500)
	}
}

func produceA(streamProducer producer.Producer, key string) {
	event := events.AA{
		ID:        uuid.New().String(),
		Type:      `aa`,
		AAA:       fmt.Sprintf(`aaa with key : %v`, key),
		Timestamp: time.Now().UnixNano() / 1e6,
	}

	encodedKey, err := encoders.StringEncoder().Encode(key)
	if err != nil {
		log.Error(err, event)
	}
	encodedVal, err := encoders.AAEncoder().Encode(event)
	if err != nil {
		log.Error(err, event)
	}

	_, _, err = streamProducer.Produce(context.Background(), &data.Record{
		Key:       encodedKey,
		Value:     encodedVal,
		Topic:     domain.ABCTopic,
		Timestamp: time.Now(),
	})

	if err != nil {
		log.Error(err)
	}
}

func produceB(streamProducer producer.Producer, key string) {
	event := events.BB{
		ID:        uuid.New().String(),
		Type:      `bb`,
		BBB:       fmt.Sprintf(`bbb with key : %v`, key),
		Timestamp: time.Now().UnixNano() / 1e6,
	}

	encodedKey, err := encoders.StringEncoder().Encode(key)
	if err != nil {
		log.Error(err, event)
	}
	encodedVal, err := encoders.BBEncoder().Encode(event)
	if err != nil {
		log.Error(err, event)
	}

	_, _, err = streamProducer.Produce(context.Background(), &data.Record{
		Key:       encodedKey,
		Value:     encodedVal,
		Topic:     domain.ABCTopic,
		Timestamp: time.Now(),
	})

	if err != nil {
		log.Error(err)
	}
}

func produceC(streamProducer producer.Producer, key string) {
	event := events.CC{
		ID:        uuid.New().String(),
		Type:      `cc`,
		CCC:       fmt.Sprintf(`ccc with key : %v`, key),
		Timestamp: time.Now().UnixNano() / 1e6,
	}

	encodedKey, err := encoders.StringEncoder().Encode(key)
	if err != nil {
		log.Error(err, event)
	}
	encodedVal, err := encoders.CCEncoder().Encode(event)
	if err != nil {
		log.Error(err, event)
	}

	_, _, err = streamProducer.Produce(context.Background(), &data.Record{
		Key:       encodedKey,
		Value:     encodedVal,
		Topic:     domain.ABCTopic,
		Timestamp: time.Now(),
	})

	if err != nil {
		log.Error(err)
	}
}

//func produceAccountDetails(streamProducer producer.Producer) {
//	for i := 1; i <= 100; i++ {
//		key := int64(i)
//		event := events.AccountDetailsUpdated{
//			ID:        uuid.New().String(),
//			Type:      `account_details_updated`,
//			Timestamp: time.Now().UnixNano() / 1e6,
//		}
//		event.Body.AccountNo = key
//		event.Body.AccountType = `Saving`
//		event.Body.CustomerID = rand.Int63n(100)
//		event.Body.Branch = `Main Branch, City A`
//		event.Body.BranchCode = 1
//		event.Body.UpdatedAt = time.Now().Unix()
//
//		encodedKey, err := encoders.KeyEncoder().Encode(key)
//		if err != nil {
//			log.Error(err, event)
//		}
//		encodedVal, err := encoders.AccountDetailsUpdatedEncoder().Encode(event)
//		if err != nil {
//			log.Error(err, event)
//		}
//
//		_, _, err = streamProducer.Produce(context.Background(), &data.Record{
//			Key:       encodedKey,
//			Value:     encodedVal,
//			Topic:     `account_detail`,
//			Timestamp: time.Now(),
//		})
//
//		if err != nil {
//			log.Error(err)
//		}
//
//		time.Sleep(time.Millisecond * 5)
//	}
//}
//
//func produceCustomerProfile(streamProducer producer.Producer) {
//
//	for i := 1; i <= 100; i++ {
//		key := int64(i)
//		event := events.CustomerProfileUpdated{
//			ID:        uuid.New().String(),
//			Type:      `customer_profile_updated`,
//			Timestamp: time.Now().UnixNano() / 1e6,
//		}
//		event.Body.CustomerID = key
//		event.Body.CustomerName = `Rob Pike`
//		event.Body.NIC = `222222222v`
//		event.Body.ContactDetails.Email = `example@gmail.com`
//		event.Body.ContactDetails.Phone = `911`
//		event.Body.ContactDetails.Address = `No 1, Lane 1, City A.`
//		event.Body.DateOfBirth = `16th-Nov-2019`
//		event.Body.UpdatedAt = time.Now().Unix()
//
//		encodedKey, err := encoders.KeyEncoder().Encode(key)
//		if err != nil {
//			log.Error(err, event)
//		}
//		encodedVal, err := encoders.CustomerProfileUpdatedEncoder().Encode(event)
//		if err != nil {
//			log.Error(err, event)
//		}
//
//		_, _, err = streamProducer.Produce(context.Background(), &data.Record{
//			Key:       encodedKey,
//			Value:     encodedVal,
//			Topic:     `customer_profile`,
//			Timestamp: time.Now(),
//		})
//
//		if err != nil {
//			log.Error(err)
//		}
//
//		time.Sleep(time.Millisecond * 5)
//	}
//}
//
//func consumeMessageAndPrint(topics *admin.Topics) {
//	mockConsumer := consumer.NewMockConsumer(topics)
//	partitions, err := mockConsumer.Consume([]string{`message`}, rebalanceHandler{})
//	if err != nil {
//		log.Fatal(`consumer error `, err)
//	}
//
//	for p := range partitions {
//		go func(pt consumer.Partition) {
//			for record := range pt.Records() {
//				log.Debug(fmt.Sprintf(`message was received to partition %v with offset %v `, record.Partition, record.Offset))
//				m, err := encoders.MessageEncoder().Decode(record.Value)
//				if err != nil {
//					log.Error(err)
//				}
//
//				message, _ := m.(events.MessageCreated)
//				fmt.Println(fmt.Sprintf(`received text message := %s`, message.Body.Text))
//				log.Info(fmt.Sprintf(`received text message := %s`, message.Body.Text))
//			}
//		}(p)
//	}
//}

type rebalanceHandler struct {
}

func (r rebalanceHandler) OnPartitionRevoked(ctx context.Context, revoked []consumer.TopicPartition) error {
	return nil
}

func (r rebalanceHandler) OnPartitionAssigned(ctx context.Context, assigned []consumer.TopicPartition) error {
	return nil
}

func produceRealData() {
	config := producer.NewConfig()
	config.Logger = log.NewLog(
		log.WithLevel(`INFO`),
		log.WithColors(true),
	).Log()

	config.BootstrapServers = []string{`localhost:9092`}

	//pro, err := producer.NewProducer(config)
	//if err != nil {
	//	log.Fatal(err)
	//}

	//produceAccountDetails(pro)
	//produceCustomerProfile(pro)
	//go produceAccountCredited(pro)
	//produceAccountDebited(pro)
}
