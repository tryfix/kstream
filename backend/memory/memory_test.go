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
	"testing"
	"time"
)

func TestMemory_Set_Expiry(t *testing.T) {
	backend := NewMemoryBackend(log.Constructor.Log(), metrics.NoopReporter())
	if err := backend.Set([]byte(`100`), []byte(`100`), 10*time.Millisecond); err != nil {
		log.Fatal(err)
	}

	time.Sleep(1 * time.Second)

	r, err := backend.Get([]byte(`100`))
	if err != nil {
		t.Error(err)
	}

	if r != nil {
		t.Error(`record exist`)
	}
}

func TestMemory_Get(t *testing.T) {
	backend := NewMemoryBackend(log.Constructor.Log(), metrics.NoopReporter())

	for i := 1; i <= 1000; i++ {
		if err := backend.Set([]byte(fmt.Sprint(i)), []byte(`100`), 0); err != nil {
			t.Fatal(err)
		}
	}

	for i := 1; i <= 1000; i++ {
		val, err := backend.Get([]byte(fmt.Sprint(i)))
		if err != nil {
			t.Error(err)
		}

		if string(val) != `100` {
			t.Fail()
		}
	}

}

func TestMemory_Delete(t *testing.T) {
	backend := NewMemoryBackend(log.Constructor.Log(), metrics.NoopReporter())

	if err := backend.Set([]byte(`100`), []byte(`100`), 0); err != nil {
		t.Fatal(err)
	}

	if err := backend.Delete([]byte(`100`)); err != nil {
		t.Fatal(err)
	}

	val, err := backend.Get([]byte(`100`))
	if err != nil {
		t.Error(err)
	}

	if val != nil {
		t.Fail()
	}

}
