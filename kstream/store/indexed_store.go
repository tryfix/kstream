package store

import (
	"context"
	nativeErrors "errors"
	"fmt"

	"github.com/tryfix/errors"
	"github.com/tryfix/kstream/kstream/encoding"

	//goEncoding "encoding"
	"sync"
	"time"
)

type Index interface {
	String() string
	Write(key, value interface{}) error
	WriteHash(hash, key interface{}) error
	Hash(key, val interface{}) (hash interface{})
	Delete(key, value interface{}) error
	Read(index interface{}) ([]interface{}, error)
	Keys() []interface{}
	Values() map[interface{}][]interface{}
	ValueIndexed(index, value interface{}) (bool, error)
}

type IndexedStore interface {
	Store
	GetIndex(ctx context.Context, name string) (Index, error)
	Indexes() []Index
	GetIndexedRecords(ctx context.Context, index string, key interface{}) ([]interface{}, error)
}

type indexedStore struct {
	Store
	indexes map[string]Index
	mu      *sync.Mutex
}

func NewIndexedStore(name string, keyEncoder, valEncoder encoding.Encoder, indexes []Index, options ...Options) (IndexedStore, error) {
	store, err := NewStore(name, keyEncoder, valEncoder, options...)
	if err != nil {
		return nil, err
	}

	idxs := make(map[string]Index)
	for _, idx := range indexes {
		idxs[idx.String()] = idx
	}

	return &indexedStore{
		Store:   store,
		indexes: idxs,
		mu:      new(sync.Mutex),
	}, nil
}

func (i *indexedStore) Set(ctx context.Context, key, val interface{}, expiry time.Duration) error {
	// set indexes
	i.mu.Lock()
	defer i.mu.Unlock()
	for _, index := range i.indexes {
		// get the previous value for the indexed key
		valPrv, err := i.Get(ctx, key)
		if err != nil {
			return err
		}

		// if previous exists and different from current value
		// eg: val.name=foo -> val.name=bar then find index for foo and delete
		if valPrv != nil {
			hash := index.Hash(key, valPrv)
			// check if value already indexed
			indexed, err := index.ValueIndexed(hash, key)
			if err != nil {
				return err
			}

			// if already indexed remove from previous index
			if indexed {
				if err := index.Delete(key, valPrv); err != nil {
					return err
				}
			}
		}

		if err := index.Write(key, val); err != nil {
			return errors.WithPrevious(err, `index write failed`)
		}
	}
	return i.Store.Set(ctx, key, val, expiry)
}

func (i *indexedStore) Delete(ctx context.Context, key interface{}) error {
	// delete indexes
	val, err := i.Store.Get(ctx, key)
	if err != nil {
		return err
	}

	i.mu.Lock()
	defer i.mu.Unlock()
	if val != nil {
		for _, index := range i.indexes {
			if err := index.Delete(key, val); err != nil {
				return err
			}
		}
	}
	return i.Store.Delete(ctx, key)
}

func (i *indexedStore) GetIndex(_ context.Context, name string) (Index, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	index, ok := i.indexes[name]
	if !ok {
		return nil, fmt.Errorf(`associate [%s] does not exist`, name)
	}
	return index, nil
}

func (i *indexedStore) Indexes() []Index {
	i.mu.Lock()
	defer i.mu.Unlock()
	var idxs []Index
	for _, idx := range i.indexes {
		idxs = append(idxs, idx)
	}
	return idxs
}

func (i *indexedStore) GetIndexedRecords(ctx context.Context, index string, key interface{}) ([]interface{}, error) {
	i.mu.Lock()
	idx, ok := i.indexes[index]
	i.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf(`associate [%s] does not exist`, index)
	}

	var records []interface{}
	indexes, err := idx.Read(key)
	if err != nil {
		if nativeErrors.Is(err, UnknownIndex) {
			return records, nil
		}
		return nil, err
	}

	for _, index := range indexes {
		record, err := i.Get(ctx, index)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}

	return records, nil
}
