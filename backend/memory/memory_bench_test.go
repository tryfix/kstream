/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package memory

import (
	"fmt"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
	"math/rand"
	"testing"
)

func BenchmarkMemory_Set(b *testing.B) {
	backend := NewMemoryBackend(log.Constructor.Log(), metrics.NoopReporter())

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := backend.Set([]byte(`100`), []byte(`100`), 0); err != nil {
				log.Fatal(err)
			}
		}
	})
}

func BenchmarkMemory_Get(b *testing.B) {
	backend := NewMemoryBackend(log.Constructor.Log(), metrics.NoopReporter())
	numOfRecs := 1000000
	for i := 1; i <= numOfRecs; i++ {
		if err := backend.Set([]byte(fmt.Sprint(i)), []byte(`100`), 0); err != nil {
			log.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			k := fmt.Sprint(rand.Intn(numOfRecs-1) + 1)

			if _, err := backend.Get([]byte(k)); err != nil {
				b.Error(err)
			}
		}
	})
}

func BenchmarkMemory_GetSet(b *testing.B) {
	backend := NewMemoryBackend(log.Constructor.Log(), metrics.NoopReporter())

	for i := 1; i <= 99999; i++ {
		if err := backend.Set([]byte(fmt.Sprint(rand.Intn(1000)+1)), []byte(`100`), 0); err != nil {
			log.Fatal(err)
		}
	}
	b.ResetTimer()
	go func() {
		for {
			if err := backend.Set([]byte(fmt.Sprint(rand.Intn(1000)+1)), []byte(`100`), 0); err != nil {
				b.Fatal(err)
			}
		}
	}()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := backend.Get([]byte(fmt.Sprint(rand.Intn(1000) + 1))); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkMemory_Iterator(b *testing.B) {
	backend := NewMemoryBackend(log.Constructor.Log(), metrics.NoopReporter())

	for i := 1; i <= 999999; i++ {
		if err := backend.Set([]byte(fmt.Sprint(rand.Intn(999999)+1)), []byte(`100`), 0); err != nil {
			log.Fatal(err)
		}
	}
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := backend.Iterator()
			for i.Valid() {

				if i.Error() != nil {
					i.Next()
					continue
				}

				i.Next()
			}
		}
	})
}
