package memory

import (
	"fmt"
	"github.com/tryfix/kstream/backend"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
	"math/rand"
	"sync"
	"testing"
)

var pMemory = NewPartitionMemoryBackend(1000, log.Constructor.Log(), metrics.NoopReporter())

func BenchmarkPartitionMemory_Set(b *testing.B) {
	pMemory := NewPartitionMemoryBackend(1000, log.Constructor.Log(), metrics.NoopReporter())

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := pMemory.Set([]byte(fmt.Sprint(rand.Intn(1000)+1)), []byte(`100`), 0); err != nil {
				log.Fatal(err)
			}
		}
	})
}

func BenchmarkPartitionMemory_Get(b *testing.B) {
	pMemory := NewPartitionMemoryBackend(100, log.Constructor.Log(), metrics.NoopReporter())

	for i := 1; i <= 10000; i++ {
		if err := pMemory.Set([]byte(fmt.Sprint(i)), []byte(`100`), 0); err != nil {
			log.Fatal(err)
		}
	}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := pMemory.Get([]byte(fmt.Sprint(rand.Intn(100) + 1))); err != nil {
				log.Fatal(err)
			}
		}
	})
}

func BenchmarkPartitionMemory_SetGet(b *testing.B) {
	pMemory := NewPartitionMemoryBackend(100, log.Constructor.Log(), metrics.NoopReporter())

	for i := 1; i <= 99999; i++ {
		if err := pMemory.Set([]byte(fmt.Sprint(rand.Intn(1000)+1)), []byte(`100`), 0); err != nil {
			log.Fatal(err)
		}
	}

	go func() {
		for {
			if err := pMemory.Set([]byte(fmt.Sprint(rand.Intn(1000)+1)), []byte(`100`), 0); err != nil {
				b.Fatal(err)
			}
		}
	}()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := pMemory.Get([]byte(fmt.Sprint(rand.Intn(1000) + 1))); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkPartitionMemory_Iterator(b *testing.B) {

	//var backend = NewPartitionMemoryBackend(1000)
	//var backend = NewPartitionMemoryBackend(100)

	for i := 1; i <= 99999; i++ {
		if err := pMemory.Set([]byte(fmt.Sprint(rand.Intn(9999)+1)), []byte(`100`), 0); err != nil {
			log.Fatal(err)
		}
	}

	//iterators := pMemory.Partitions()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			/*it := pMemory.Iterator()
			for it.Valid() {

				if it.Error() != nil {
					it.Next()
					continue
				}

				_, err := encoders.DriverLocationSyncEncoderBuilder().Decode(it.Value())
				if err != nil {
					it.Next()
					continue
				}

				it.Next()
			}*/
			iterators := pMemory.Partitions()
			wg := new(sync.WaitGroup)
			for _, i := range iterators {
				wg.Add(1)
				go func(it backend.Iterator, wg *sync.WaitGroup) {
					defer wg.Done()
					for it.Valid() {

						if it.Error() != nil {
							it.Next()
							continue
						}

						it.Next()
						continue

						it.Next()
					}
				}(i, wg)
			}

			wg.Wait()
		}
	})

}
