package memory

import (
	"github.com/tryfix/kstream/backend"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
	"strconv"
	"time"
)

type PartitionMemory interface {
	backend.Backend
	Partitions() []backend.Iterator
}

type partitionMemory struct {
	partitionCount int
	partitions     map[int]backend.Backend
}

func NewPartitionMemoryBackend(partitions int, logger log.Logger, reporter metrics.Reporter) PartitionMemory {
	partitionedBackend := &partitionMemory{
		partitionCount: partitions,
		partitions:     make(map[int]backend.Backend),
	}

	for i := 0; i < partitions; i++ {
		backend := NewMemoryBackend(logger, reporter)
		partitionedBackend.partitions[i] = backend
	}

	return partitionedBackend
}

func (pm *partitionMemory) Name() string {
	return `partitioned_memory_backend`
}

func (pm *partitionMemory) Set(key []byte, value []byte, expiry time.Duration) error {
	k, err := strconv.Atoi(string(key))
	if err != nil {
		return err
	}
	partitionId := k % pm.partitionCount

	pm.partitions[partitionId].Set(key, value, expiry)

	return nil
}

func (pm *partitionMemory) Get(key []byte) ([]byte, error) {
	k, err := strconv.Atoi(string(key))
	if err != nil {
		return nil, err
	}

	partitionId := k % pm.partitionCount

	return pm.partitions[partitionId].Get(key)
}

func (pm *partitionMemory) RangeIterator(fromKy []byte, toKey []byte) backend.Iterator {
	panic("implement me")
}

func (pm *partitionMemory) Iterator() backend.Iterator {
	panic("implement me")
}

func (pm *partitionMemory) Delete(key []byte) error {

	partitionId, err := strconv.Atoi(string(key))
	if err != nil {
		return err
	}
	partitionId = partitionId % pm.partitionCount

	return pm.partitions[partitionId].Delete(key)
}

func (m *partitionMemory) Destroy() error { return nil }

func (pm *partitionMemory) SetExpiry(time time.Duration) {}

func (pm *partitionMemory) String() string {
	return `partition memory`
}

func (pm *partitionMemory) Persistent() bool {
	return false
}

func (pm *partitionMemory) Close() error {
	for i := 0; i < pm.partitionCount; i++ {
		pm.partitions[i].Close()
	}
	return nil
}

func (pm *partitionMemory) Partitions() []backend.Iterator {
	var iterators []backend.Iterator
	for _, partition := range pm.partitions {
		iterators = append(iterators, partition.Iterator())
	}
	return iterators
}
