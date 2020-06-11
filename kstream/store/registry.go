package store

import (
	"errors"
	"fmt"
	"github.com/tryfix/kstream/kstream/encoding"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
	"sync"
)

type Registry interface {
	Register(store Store)
	New(name string, keyEncoder, valEncoder encoding.Builder, options ...Options) Store
	NewIndexedStore(name string, keyEncoder, valEncoder encoding.Builder, indexes []Index, options ...Options) IndexedStore
	Store(name string) (Store, error)
	Index(name string) (Index, error)
	Stores() []Store
	Indexes() []Index
}

type registry struct {
	stores              map[string]Store
	stateStores         map[string]StateStore
	indexes             map[string]Index
	mu                  *sync.Mutex
	logger              log.Logger
	applicationId       string
	storeBuilder        Builder
	indexedStoreBuilder IndexedStoreBuilder
	stateStoreBuilder   StateStoreBuilder
}

type RegistryConfig struct {
	Host                string
	HttpEnabled         bool
	applicationId       string
	StoreBuilder        Builder
	StateStoreBuilder   StateStoreBuilder
	IndexedStoreBuilder IndexedStoreBuilder
	Logger              log.Logger
	MetricsReporter     metrics.Reporter
}

func NewRegistry(config *RegistryConfig) Registry {
	reg := &registry{
		stores:              make(map[string]Store),
		stateStores:         make(map[string]StateStore),
		indexes:             make(map[string]Index),
		mu:                  &sync.Mutex{},
		logger:              config.Logger.NewLog(log.Prefixed(`store-registry`)),
		applicationId:       config.applicationId,
		stateStoreBuilder:   config.StateStoreBuilder,
		indexedStoreBuilder: config.IndexedStoreBuilder,
		storeBuilder:        config.StoreBuilder,
	}

	if config.HttpEnabled {
		MakeEndpoints(config.Host, reg, reg.logger.NewLog(log.Prefixed(`http`)))
	}

	return reg
}

func (r *registry) Register(store Store) {
	name := store.Name()
	if _, ok := r.stores[name]; ok {
		r.logger.Fatal(fmt.Sprintf(`store [%s] already exist`, name))
	}

	// if store is an IndexedStore store register Indexes
	if stor, ok := store.(IndexedStore); ok {
		for _, idx := range stor.Indexes() {
			r.indexes[idx.String()] = idx
		}
	}

	r.stores[name] = store
}

func (r *registry) New(name string, keyEncoder encoding.Builder, valEncoder encoding.Builder, options ...Options) Store {
	if _, ok := r.stores[name]; ok {
		r.logger.Fatal(fmt.Sprintf(`store [%s] already exist`, name))
	}

	s, err := r.storeBuilder(name, keyEncoder, valEncoder, options...)
	if err != nil {
		r.logger.Fatal(err)
	}

	r.stores[name] = s

	return r.stores[name]
}

func (r *registry) NewIndexedStore(name string, keyEncoder, valEncoder encoding.Builder, indexes []Index, options ...Options) IndexedStore {
	if _, ok := r.stores[name]; ok {
		r.logger.Fatal(fmt.Sprintf(`store [%s] already exist`, name))
	}

	s, err := r.indexedStoreBuilder(name, keyEncoder, valEncoder, indexes, options...)
	if err != nil {
		r.logger.Fatal(err)
	}

	r.stores[name] = s

	for _, idx := range s.Indexes() {
		r.indexes[idx.String()] = idx
	}

	return s
}

func (r *registry) Store(name string) (Store, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	store, ok := r.stores[name]
	if !ok {
		return nil, errors.New(fmt.Sprintf(`unknown store [%s]`, name))
	}

	return store, nil
}

func (r *registry) Index(name string) (Index, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	idx, ok := r.indexes[name]
	if !ok {
		return nil, errors.New(fmt.Sprintf(`unknown index [%s]`, name))
	}

	return idx, nil
}

func (r *registry) Stores() []Store {
	var list []Store

	for _, stor := range r.stores {
		list = append(list, stor)
	}

	return list
}

func (r *registry) Indexes() []Index {
	var list []Index

	for _, idx := range r.indexes {
		list = append(list, idx)
	}

	return list
}
