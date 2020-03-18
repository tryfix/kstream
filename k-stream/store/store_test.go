package store

import (
	"context"
	"github.com/tryfix/kstream/backend"
	"github.com/tryfix/kstream/k-stream/encoding"
	"testing"
	"time"
)

func makeTestStore(expiry time.Duration) Store {
	return &store{
		backend:    backend.NewMockBackend(`test_backend`, expiry),
		name:       `test_store`,
		keyEncoder: encoding.IntEncoder{},
		valEncoder: encoding.StringEncoder{},
	}
}

func TestDefaultStore_Get(t *testing.T) {
	ctx := context.Background()
	st := makeTestStore(0)
	testValue := `test_value`
	err := st.Set(ctx, 100, testValue, 0)
	if err != nil {
		t.Error(err)
		return
	}

	v, err := st.Get(ctx, 100)
	if err != nil {
		t.Error(err)
	}

	if v != testValue {
		t.Fail()
	}
}

func TestDefaultStore_Get_Should_return_Nul_For_Invalid_Key(t *testing.T) {
	ctx := context.Background()
	st := makeTestStore(0)
	testValue := `test_value`
	testKey := 100
	err := st.Set(ctx, testKey, testValue, 0)
	if err != nil {
		t.Error(err)
		return
	}

	v, err := st.Get(ctx, 200)
	if err != nil {
		t.Error(err)
	}

	if v != nil {
		t.Fail()
	}
}

func TestDefaultStore_Set(t *testing.T) {
	ctx := context.Background()
	st := makeTestStore(0)
	testValue := `test_value`
	testKey := 100
	err := st.Set(ctx, testKey, testValue, 0)
	if err != nil {
		t.Error(err)
		return
	}

	v, err := st.Get(ctx, testKey)
	if err != nil {
		t.Error(err)
	}

	if v != testValue {
		t.Fail()
	}
}

func TestDefaultStore_Delete(t *testing.T) {
	ctx := context.Background()
	st := makeTestStore(0)
	testValue := `test_value`
	testKey := 100
	err := st.Set(ctx, testKey, testValue, 0)
	if err != nil {
		t.Error(err)
		return
	}

	v, err := st.Get(ctx, testKey)
	if err != nil {
		t.Error(err)
	}

	if v != testValue {
		t.Fail()
	}
}

func TestDefaultStore_Set_Record_Expiry(t *testing.T) {
	ctx := context.Background()
	st := makeTestStore(0)
	testValue := `test_value`
	testKey := 100
	expiry := 100 * time.Millisecond
	err := st.Set(ctx, testKey, testValue, expiry)
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(expiry * 2)

	v, err := st.Get(ctx, testKey)
	if err != nil {
		t.Error(err)
	}

	if v != nil {
		t.Fail()
	}
}

func TestDefaultStore_Set_Store_Expiry(t *testing.T) {
	ctx := context.Background()
	expiry := 100 * time.Millisecond
	st := makeTestStore(expiry)
	testValue := `test_value`
	testKey := 100
	err := st.Set(ctx, testKey, testValue, 0)
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(expiry * 2)

	v, err := st.Get(ctx, testKey)
	if err != nil {
		t.Error(err)
	}

	if v != nil {
		t.Fail()
	}
}
