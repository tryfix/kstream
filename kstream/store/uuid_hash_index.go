package store

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/tryfix/errors"
	"reflect"
	"sync"
)

type uuidHashIndex struct {
	indexes map[uuid.UUID]map[interface{}]bool // indexKey:recordKey:bool
	mapper  func(key, val interface{}) (idx uuid.UUID)
	mu      *sync.Mutex
	name    string
}

func NewUuidHashIndex(name string, mapper func(key, val interface{}) (idx uuid.UUID)) Index {
	return &uuidHashIndex{
		indexes: make(map[uuid.UUID]map[interface{}]bool),
		mapper:  mapper,
		mu:      new(sync.Mutex),
		name:    name,
	}
}

func (s *uuidHashIndex) String() string {
	return s.name
}

func (s *uuidHashIndex) Write(key, value interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	hashKey := s.mapper(key, value)
	_, ok := s.indexes[hashKey]
	if !ok {
		s.indexes[hashKey] = make(map[interface{}]bool)
	}
	s.indexes[hashKey][key] = true
	return nil
}

func (s *uuidHashIndex) ValueIndexed(index, value interface{}) (bool, error) {
	hStr, ok := index.(uuid.UUID)
	if !ok {
		return false, errors.New(fmt.Sprintf(`unsupported hash type expected [string] given [%s]`, reflect.TypeOf(index)))
	}
	_, ok = s.indexes[hStr]
	if !ok {
		return false, nil
	}

	_, ok = s.indexes[hStr][value]
	return ok, nil
}

func (s *uuidHashIndex) Hash(key, val interface{}) (hash interface{}) {
	return s.mapper(key, val)
}

func (s *uuidHashIndex) WriteHash(hash, key interface{}) error {
	hStr, ok := hash.(uuid.UUID)
	if !ok {
		return errors.New(fmt.Sprintf(`unsupported hash type expected [string] given [%s]`, reflect.TypeOf(hash)))
	}
	_, ok = s.indexes[hStr]
	if !ok {
		s.indexes[hStr] = make(map[interface{}]bool)
	}
	s.indexes[hStr][key] = true

	return nil
}

func (s *uuidHashIndex) Delete(key, value interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	hashKey := s.mapper(key, value)
	if _, ok := s.indexes[hashKey]; !ok {
		return fmt.Errorf(`hashKey %s does not exist for %s`, hashKey, s.name)
	}

	delete(s.indexes[hashKey], key)
	return nil
}

func (s *uuidHashIndex) Keys() []interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	var keys []interface{}

	for key := range s.indexes {
		keys = append(keys, key)
	}

	return keys
}

func (s *uuidHashIndex) Values() map[interface{}][]interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	values := make(map[interface{}][]interface{})

	for idx, keys := range s.indexes {
		for key := range keys {
			values[idx] = append(values[idx], key)
		}
	}

	return values
}

func (s *uuidHashIndex) Read(key interface{}) ([]interface{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var indexes []interface{}
	index, ok := s.indexes[key.(uuid.UUID)]
	if !ok {
		return nil, UnknownIndex
	}
	for k := range index {
		indexes = append(indexes, k)
	}

	return indexes, nil
}
