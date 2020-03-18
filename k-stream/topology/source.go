package topology

import (
	"context"
)

type SourceBuilder interface {
	Name() string
	Info() map[string]string
	SourceType() string
	Build() (Source, error)
}

type SinkBuilder interface {
	NodeBuilder
	Name() string
	ID() int32
	Info() map[string]string
	SinkType() string
}

type Source interface {
	Run(ctx context.Context, kIn, vIn []byte) (kOut, vOut interface{}, err error)
	Name() string
	Close()
}

type Sink interface {
	Node
	Name() string
	Close() error
}
