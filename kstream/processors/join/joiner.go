package join

import (
	"context"
	"github.com/tryfix/kstream/kstream/topology"
)

type Type int

const (
	LeftJoin Type = iota
	InnerJoin
)

type Joiner interface {
	topology.Node
	Join(ctx context.Context, key, val interface{}) (joinedVal interface{}, err error)
}

type KeyMapper func(key, value interface{}) (mappedKey interface{}, err error)

type ValueMapper func(left, right interface{}) (joined interface{}, err error)
