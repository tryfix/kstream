package window

import "github.com/tryfix/kstream/k-stream/context"

type Window interface {
	Store(ctx context.Context, key, value interface{}) error
	Get(ctx context.Context, key interface{}) (value interface{}, err error)
}

//type slidingWindow
