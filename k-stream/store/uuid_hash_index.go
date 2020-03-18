package store

import (
	"fmt"
	"github.com/google/uuid"
	"sync"
)

type uuidHashIndex struct {
	indexes map[uuid.UUID]map[uuid.UUID]bool // indexKey:recordKey:bool
	mapper  func(key, val interface{}) (idx uuid.UUID)
	mu      *sync.Mutex
	name    string
}

func NewUuidHashIndex(name string, mapper func(key, val interface{}) (idx uuid.UUID)) Index {
	return &uuidHashIndex{
		indexes: make(map[uuid.UUID]map[uuid.UUID]bool),
		mapper:  mapper,
		mu:      new(sync.Mutex),
		name:    name,
	}
}

func (s *uuidHashIndex) Name() string {
	return s.name
}

func (s *uuidHashIndex) Write(key, value interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	hashKey := s.mapper(key, value)
	_, ok := s.indexes[hashKey]
	if !ok {
		s.indexes[hashKey] = make(map[uuid.UUID]bool)
	}
	s.indexes[hashKey][key.(uuid.UUID)] = true
	return nil
}

func (s *uuidHashIndex) Delete(key, value interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	hashKey := s.mapper(key, value)
	if _, ok := s.indexes[hashKey]; !ok {
		return fmt.Errorf(`hashKey %s does not exist for %s`, hashKey, s.name)
	}

	delete(s.indexes[hashKey], key.(uuid.UUID))
	return nil
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
