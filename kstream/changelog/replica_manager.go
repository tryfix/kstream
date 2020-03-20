package changelog

import (
	"fmt"
	"github.com/tryfix/kstream/backend"
	"github.com/tryfix/kstream/consumer"
	"github.com/tryfix/kstream/kstream/offsets"
	"github.com/tryfix/log"
	"sync"
)

type ReplicaManager struct {
	replicas      map[string]*replicaSyncer // map[tp]syncer
	mu            *sync.RWMutex
	offsetManager offsets.Manager
	conf          *ReplicaManagerConf
	cacheManager  *cacheManager
	logger        log.Logger
}

type ReplicaManagerConf struct {
	Consumer      consumer.PartitionConsumerBuilder
	Backend       backend.Builder
	Logger        log.Logger
	Tps           []consumer.TopicPartition
	OffsetManager offsets.Manager
}

func NewReplicaManager(conf *ReplicaManagerConf) (*ReplicaManager, error) {

	rm := &ReplicaManager{
		replicas:      make(map[string]*replicaSyncer),
		mu:            new(sync.RWMutex),
		offsetManager: conf.OffsetManager,
		conf:          conf,
		logger:        conf.Logger.NewLog(log.Prefixed(`replica-manager`)),
	}

	cacheManager, err := newCacheManager(conf.Backend)
	if err != nil {
		return nil, err
	}

	rm.cacheManager = cacheManager

	for _, tp := range conf.Tps {
		replica, err := rm.buildReplica(tp)
		if err != nil {
			return nil, err
		}
		rm.replicas[tp.String()] = replica
	}

	return rm, nil
}

func (m *ReplicaManager) StartReplicas(tps []consumer.TopicPartition) error {
	m.logger.Info(`starting replica syncers...`)
	wg := new(sync.WaitGroup)
	for _, tp := range tps {
		wg.Add(1)
		if err := m.startReplica(tp, wg); err != nil {
			return err
		}
	}
	wg.Wait()
	return nil
}

func (m *ReplicaManager) GetCache(tp consumer.TopicPartition) (*cache, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cache, err := m.cacheManager.getCache(tp)
	if err != nil {
		return nil, err
	}

	return cache, nil
}

func (m *ReplicaManager) startReplica(tp consumer.TopicPartition, wg *sync.WaitGroup) error {
	m.logger.Info(fmt.Sprintf(`starting replica syncer [%s]...`, tp))

	replica := m.replicas[tp.String()]

	localCached, err := replica.cache.LastSynced()
	if err != nil {
		return err
	}

	valid, err := m.offsetManager.OffsetValid(tp.Topic, tp.Partition, localCached)
	if err != nil {
		return err
	}
	startingOffset := localCached

	if !valid {
		broker, err := m.offsetManager.GetOffsetOldest(tp.Topic, tp.Partition)
		if err != nil {
			return err
		}
		startingOffset = broker
	}

	if valid {
		startingOffset = localCached
		m.logger.Info(fmt.Sprintf(`local cache [%d] found for [%s]`, localCached, tp))
	} else {
		m.logger.Info(fmt.Sprintf(`local cache invalid for [%s] flushing...`, tp))
		if err := replica.cache.Flush(); err != nil {
			return err
		}
		m.logger.Info(fmt.Sprintf(`local cache flushed for [%s]`, tp))
	}

	started, errs := replica.Sync(startingOffset)
	go func() {
		for err := range errs {
			m.logger.Fatal(err)
		}
	}()

	<-started
	wg.Done()
	m.logger.Info(fmt.Sprintf(`started replica syncer [%s]`, tp))

	return nil
}

func (m *ReplicaManager) buildReplica(tp consumer.TopicPartition) (*replicaSyncer, error) {
	c, err := m.conf.Consumer.Build(consumer.BuilderWithId(fmt.Sprintf(`changelog_%s_replica_consumer`, tp)))
	if err != nil {
		return nil, err
	}

	if rep, ok := m.replicas[tp.String()]; ok {
		rep.consumer = c
		return rep, nil
	}

	cache, err := m.cacheManager.getCache(tp)
	if err != nil {
		return nil, err
	}

	return &replicaSyncer{
		cache:    cache,
		tp:       tp,
		consumer: c,
		logger:   m.logger.NewLog(log.Prefixed(fmt.Sprintf(`sync-%s-%d`, tp.Topic, tp.Partition))),
		running:  true,
	}, nil
}

func (m *ReplicaManager) StopReplicas(tps []consumer.TopicPartition) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.logger.Info(`stopping replica sync...`)
	for _, tp := range tps {
		r := m.replicas[tp.String()]
		if err := r.Stop(); err != nil {
			m.logger.Error(err)
		}
	}
	m.logger.Info(`replica sync running`)
	return nil
}
