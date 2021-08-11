package kstream

import (
	"fmt"
	"github.com/tryfix/errors"
	"github.com/tryfix/kstream/consumer"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
	"sync"
)

type StreamInstance struct {
	id                     string
	streams                map[string]*kStream // topic:topology
	topics                 []string
	processorPool          *processorPool
	numOfParallelConsumers int
	stopping               chan bool
	stopped                chan bool
	logger                 log.Logger
	consumer               consumer.Builder
	builder                *StreamBuilder
	instancesOptions       *instancesOptions
	reBalanceHandler       consumer.ReBalanceHandler
}

type instancesOptions struct {
	notifyOnSynced   chan bool
	reBalanceHandler consumer.ReBalanceHandler
	consumerOptions  []consumer.Option
}

func NotifyOnStart(c chan bool) InstancesOptions {
	return func(config *instancesOptions) {
		config.notifyOnSynced = c
	}
}

func WithReBalanceHandler(h consumer.ReBalanceHandler) InstancesOptions {
	return func(config *instancesOptions) {
		config.reBalanceHandler = h
	}
}

func WithConsumerOptions(opt consumer.Option) InstancesOptions {
	return func(config *instancesOptions) {
		config.consumerOptions = append(config.consumerOptions, opt)
	}
}

type InstancesOptions func(config *instancesOptions)

func (iOpts *instancesOptions) apply(options ...InstancesOptions) {
	for _, o := range options {
		o(iOpts)
	}
}

type Instances struct {
	streams           map[int]*StreamInstance
	globalTables      map[string]*GlobalKTable
	globalTableStream *globalTableStream
	options           *instancesOptions
	logger            log.Logger
	builder           *StreamBuilder
	metricsReporter   metrics.Reporter
}

func NewStreams(builder *StreamBuilder, options ...InstancesOptions) *Instances {

	opts := new(instancesOptions)
	opts.apply(options...)

	instances := &Instances{
		streams:         make(map[int]*StreamInstance),
		globalTables:    builder.globalTables,
		options:         opts,
		builder:         builder,
		metricsReporter: builder.metricsReporter,
		logger:          builder.logger.NewLog(log.Prefixed(`streams`)),
	}

	if len(builder.streams) < 1 {
		return instances
	}

	for i := 1; i <= builder.config.ConsumerCount; i++ {
		id := fmt.Sprintf(`streams-%d`, i)
		logger := builder.logger.NewLog(log.Prefixed(id))
		instance := &StreamInstance{
			id:                     id,
			streams:                builder.streams,
			processorPool:          newProcessorPool(id, builder.streams, builder.defaultBuilders.changelog, logger, builder.metricsReporter),
			numOfParallelConsumers: builder.config.ConsumerCount,
			stopping:               make(chan bool, 1),
			stopped:                make(chan bool, 1),
			logger:                 logger,
			consumer:               builder.defaultBuilders.Consumer,
			builder:                builder,
			instancesOptions:       opts,
		}

		if opts.reBalanceHandler != nil {
			instance.reBalanceHandler = opts.reBalanceHandler
		}

		for t := range builder.streams {
			instance.topics = append(instance.topics, t)
		}

		instances.streams[i] = instance
	}

	return instances
}

func (ins *Instances) Start() (err error) {

	defer ins.logger.Info(`k-stream shutdown completed`)

	/*var syncableReplicas []consumer.TopicPartition

	// start changelog replica syncing
	for _, stream := range ins.builder.streams {
		if stream.changelog.enabled && stream.changelog.replicated {
			for i := 0; i <= stream.changelog.topic.numOfPartitions-1; i++ {
				// already allocated partition doesnt need a replica syncer
				syncableReplicas = append(syncableReplicas, consumer.TopicPartition{
					Topic: stream.changelog.topic.name, Partition: int32(i),
				})
			}
		}
	}

	if len(syncableReplicas) > 0 {
		if err := ins.builder.changelogReplicaManager.StartReplicas(syncableReplicas); err != nil {
			ins.logger.Fatal( err)
		}
	}*/

	wg := new(sync.WaitGroup)
	// start global table streams
	if len(ins.globalTables) > 0 {
		//wg.Add(1)
		ins.globalTableStream, err = newGlobalTableStream(ins.globalTables, &GlobalTableStreamConfig{
			ConsumerBuilder: ins.builder.defaultBuilders.PartitionConsumer,
			Logger:          ins.logger,
			KafkaAdmin:      ins.builder.defaultBuilders.KafkaAdmin,
			OffsetManager:   ins.builder.defaultBuilders.OffsetManager,
			Metrics:         ins.metricsReporter,
			BackendBuilder:  ins.builder.defaultBuilders.Backend,
		})
		if err != nil {
			return errors.WithPrevious(err, `global table stream start failed`)
		}
		ins.globalTableStream.StartStreams(wg)

		if len(ins.streams) < 1 {
			if ins.options.notifyOnSynced != nil {
				ins.options.notifyOnSynced <- true
			}

			wg.Wait()
			return nil
		}
	}

	for _, instance := range ins.streams {
		// start stream consumer
		wg.Add(1)
		go func(wg *sync.WaitGroup, i *StreamInstance) {
			if err := i.Start(wg); err != nil {
				ins.logger.Fatal(err)
			}
		}(wg, instance)
	}

	if ins.options.notifyOnSynced != nil {
		ins.options.notifyOnSynced <- true
	}

	wg.Wait()

	return nil
}

// Stop stops all the running Streams Instances and then GlobalTables
func (ins *Instances) Stop() {
	if len(ins.streams) > 0 {
		// stop all the streams first
		wg := &sync.WaitGroup{}
		for _, instance := range ins.streams {
			wg.Add(1)
			go func(w *sync.WaitGroup, i *StreamInstance) {
				i.Stop()
				w.Done()
			}(wg, instance)
		}
		wg.Wait()
	}

	// stop running global tables
	if len(ins.globalTables) > 0 {
		ins.globalTableStream.stop()
	}

}

// starts the high level consumer for all streams
func (s *StreamInstance) Start(wg *sync.WaitGroup) error {

	defer wg.Done()
	/*config.OnRebalanced = func(allocation consumer.Allocation) {
		// consumer booting logic
		if len(allocation.Assigned) > 0 {
			logger.DefaultLogger.Warn( `partitions added`, allocation.Assigned)

			for _, tp := range allocation.Assigned {

				// stop started replicas
				if s.streams[tp.Topic].changelog.replicated {
					if err := s.builder.changelogReplicaManager.StopReplicas([]consumer.TopicPartition{
						{Topic: s.streams[tp.Topic].changelog.topic.name, Partition: tp.Partition},
					}); err != nil {
						logger.DefaultLogger.Error( err)
					}
				}

				if err := s.processorPool.addProcessor(tp); err != nil {
					logger.DefaultLogger.Fatal( `allocation failed due to `, err)
				}

				pro := s.processorPool.Processor(tp)
				if err := pro.boot(); err != nil {
					logger.DefaultLogger.Fatal(`k-stream.consumer`,
						fmt.Sprintf("cannot boot processor due to : %+v", err))
				}
			}
		}

		if len(allocation.Removed) > 0 {
			logger.DefaultLogger.Warn( `partitions removed`, allocation.Removed)
			// start changelog replica syncers
			var syncableReplicas []consumer.TopicPartition
			for _, tp := range allocation.Removed {
				if s.streams[tp.Topic].changelog.replicated {
					syncableReplicas = append(syncableReplicas, consumer.TopicPartition{
						Topic: s.streams[tp.Topic].changelog.topic.name, Partition: tp.Partition,
					})
				}
			}

			if len(syncableReplicas) > 0 {
				if err := s.builder.changelogReplicaManager.StartReplicas(syncableReplicas); err != nil {
					logger.DefaultLogger.Error( fmt.Sprintf(`cannot start replicas due to %s`, err))
				}
			}
		}

		go func() {
			if s.allocationNotify != nil {
				s.allocationNotify <- allocation
			}
		}()

	}*/

	c, err := s.consumer.Build(
		consumer.BuilderWithId(fmt.Sprintf(`group_consumer_%s`, s.id)),
		consumer.BuilderWithOptions(s.instancesOptions.consumerOptions...),
	)
	if err != nil {
		return errors.WithPrevious(err, `cannot initiate consumer`)
	}

	reBalancer := &reBalanceHandler{
		userHandler: s.reBalanceHandler,
		logger:      s.logger,
		processors:  s.processorPool,
		builder:     s.builder,
	}

	partitionConsumers, err := c.Consume(s.topics, reBalancer)
	if err != nil {
		return errors.WithPrevious(err, `cannot start consumer`)
	}

	consumerWg := new(sync.WaitGroup)
	consumerWg.Add(1)
	go func(consumerWg *sync.WaitGroup) {
		<-s.stopping
		if err := c.Close(); err != nil {
			s.logger.Fatal(err)
		}
		consumerWg.Done()
		s.stopped <- true
	}(consumerWg)

	go func() {
		for err := range c.Errors() {
			s.logger.Info(err)
		}
	}()

	wgx := new(sync.WaitGroup)
	for partition := range partitionConsumers {
		wgx.Add(1)
		go func(wg *sync.WaitGroup, p consumer.Partition) {
			pro := s.processorPool.Processor(p.Partition())

			go func(processor *processor) {
				for record := range processor.changelogMarks {
					if err := p.CommitOffset(record); err != nil {
						s.logger.Error(fmt.Sprintf("cannot commit partition offset due to : %+v", err))
					}
				}
			}(pro)

			pro.records = p.Records()
			pro.start()
			wg.Done()
		}(wgx, partition)
	}

	wgx.Wait()
	consumerWg.Wait()
	return nil
}

func (s *StreamInstance) Stop() {
	s.stopping <- true
	<-s.stopped
	s.logger.Info(`instance stopped`)
}
