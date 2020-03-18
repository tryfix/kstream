package processors

import (
	"context"
	"errors"
	"testing"
)

var f FilterFunc = func(ctx context.Context, key, value interface{}) (b bool, err error) {
	k, ok := key.(int)
	if !ok {
		return false, errors.New(`invalid type`)
	}
	return k == 1, nil
}

var filter = &Filter{
	FilterFunc: f,
}

func TestFilter_Process_Should_Filter(t *testing.T) {
	k, v, next, err := filter.Run(context.Background(), 1, nil)
	if err != nil {
		t.Error(err)
	}
	if !next {
		t.Fail()
	}

	if k != 1 {
		t.Fail()
	}

	if v != nil {
		t.Fail()
	}
}

func TestFilter_Process_Should_Return_Org_Vals_On_Error(t *testing.T) {
	kOrg := `100`
	vOrg := `100`

	k, v, next, err := filter.Run(context.Background(), kOrg, vOrg)
	if err == nil {
		t.Fail()
	}

	if next {
		t.Fail()
	}

	if k != nil {
		t.Fail()
	}

	if v != nil {
		t.Fail()
	}
}
