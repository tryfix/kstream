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
	New(name string, keyEncoder encoding.Builder, valEncoder encoding.Builder, options ...Options) Store
	Store(name string) (Store, error)
	List() []string
}

type registry struct {
	Stores            map[string]Store
	StateStores       map[string]StateStore
	mu                *sync.Mutex
	logger            log.Logger
	applicationId     string
	storeBuilder      Builder
	stateStoreBuilder StateStoreBuilder
}

type RegistryConfig struct {
	Host              string
	HttpEnabled       bool
	applicationId     string
	StoreBuilder      Builder
	StateStoreBuilder StateStoreBuilder
	Logger            log.Logger
	MetricsReporter   metrics.Reporter
}

func NewRegistry(config *RegistryConfig) Registry {
	reg := &registry{
		Stores:            make(map[string]Store),
		StateStores:       make(map[string]StateStore),
		mu:                &sync.Mutex{},
		logger:            config.Logger.NewLog(log.Prefixed(`store-registry`)),
		applicationId:     config.applicationId,
		stateStoreBuilder: config.StateStoreBuilder,
		storeBuilder:      config.StoreBuilder,
	}

	if config.HttpEnabled {
		MakeEndpoints(config.Host, reg, reg.logger.NewLog(log.Prefixed(`http`)))
	}

	return reg
}

func (r *registry) Register(store Store) {
	name := store.Name()
	if _, ok := r.Stores[name]; ok {
		r.logger.Fatal(fmt.Sprintf(`store [%s] already exist`, name))
	}

	r.Stores[name] = store
}

func (r *registry) New(name string, keyEncoder encoding.Builder, valEncoder encoding.Builder, options ...Options) Store {
	if _, ok := r.Stores[name]; ok {
		r.logger.Fatal(fmt.Sprintf(`store [%s] already exist`, name))
	}

	s, err := r.storeBuilder(name, keyEncoder, valEncoder, options...)
	if err != nil {
		r.logger.Fatal(err)
	}

	r.Stores[name] = s

	return r.Stores[name]
}

func (r *registry) Store(name string) (Store, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	store, ok := r.Stores[name]
	if !ok {
		return nil, errors.New(fmt.Sprintf(`unknown store [%s]`, name))
	}

	return store, nil
}

func (r *registry) List() []string {
	var list []string

	for name := range r.Stores {
		list = append(list, name)
	}

	return list
}
