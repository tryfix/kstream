package processors

import (
	"context"
	"errors"
	"testing"
)

var tr TransFunc = func(ctx context.Context, key interface{}, value interface{}) (interface{}, interface{}, error) {
	k, ok := key.(int)
	if !ok {
		return nil, nil, errors.New(`invalid key`)
	}

	v, ok := value.(string)
	if !ok {
		return nil, nil, errors.New(`invalid key`)
	}

	k *= 10
	v += `test`

	return k, v, nil
}

var transformer = Transformer{
	TransFunc: tr,
}

func TestTransformer_Process_Should_Transform(t *testing.T) {

	k, v, _, err := transformer.Run(context.Background(), 1, `1`)
	if err != nil {
		t.Fail()
	}

	if k != 10 {
		t.Fail()
	}

	if v != `1test` {
		t.Fail()
	}

}

func TestTransformer_Process_Should_Not_Transform_On_Error(t *testing.T) {
	keyOrg := `10`
	valOrg := 10
	k, v, _, err := transformer.Run(context.Background(), keyOrg, valOrg)
	if err == nil {
		t.Fail()
	}

	if k != nil || v != nil {
		t.Fail()
	}
}
