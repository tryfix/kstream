package backend

import (
	"sync"
	"time"
)

type MockBackend struct {
	name   string
	data   map[string][]byte
	mu     *sync.Mutex
	expiry time.Duration
}

func NewMockBackend(name string, expiry time.Duration) Backend {
	b := &MockBackend{
		name: name,
		data: make(map[string][]byte),
		mu:   new(sync.Mutex),
	}

	if expiry > 0 {
		b.expiry = expiry
	}

	return b
}

func (b *MockBackend) Name() string {
	return b.name
}

func (b *MockBackend) Persistent() bool {
	return false
}

func (b *MockBackend) Set(key []byte, value []byte, expiry time.Duration) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	var del = func() {
		time.Sleep(expiry)
		b.Delete(key)
	}

	if expiry > 0 {
		go del()
	} else {
		if b.expiry > 0 {
			go del()
		}
	}

	b.data[string(key)] = value
	return nil
}

func (b *MockBackend) Get(key []byte) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	v, ok := b.data[string(key)]
	if !ok {
		return nil, nil
	}

	return v, nil
}

func (b *MockBackend) RangeIterator(fromKy []byte, toKey []byte) Iterator {
	panic("implement me")
}

func (*MockBackend) Iterator() Iterator {
	panic("implement me")
}

func (b *MockBackend) Delete(key []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.data, string(key))
	return nil
}

func (b *MockBackend) SetExpiry(time time.Duration) {
	b.expiry = time
}

func (b *MockBackend) String() string {
	return b.name
}

func (*MockBackend) Close() error {
	panic("implement me")
}

func (*MockBackend) Destroy() error {
	panic("implement me")
}
