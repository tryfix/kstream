package processors

import (
	"context"
	"github.com/tryfix/errors"
	"github.com/tryfix/kstream/kstream/store"
	"github.com/tryfix/kstream/kstream/topology"
)

type RecordVersionExtractor func(ctx context.Context, key, value interface{}) (version int64, err error)
type RecordVersionWriter func(ctx context.Context, version int64, vIn interface{}) (vOut interface{}, err error)

type Materializer struct {
	Topic            string
	Id               int32
	Store            string
	VersionExtractor RecordVersionExtractor
	VersionWriter    RecordVersionWriter
	store            store.Store
	Registry         store.Registry
	childBuilders    []topology.NodeBuilder
	childs           []topology.Node
}

func NewMaterializeBuilder(topic, store string, id int32, options ...MaterializeOption) *Materializer {
	builder := &Materializer{
		Topic: topic,
		Id:    id,
		Store: store,
		VersionWriter: func(ctx context.Context, version int64, vIn interface{}) (vOut interface{}, err error) {
			return vIn, nil
		},
		//VersionExtractor: func(key, value interface{}) (version int64, err error) {
		//	return 1, nil
		//},
	}

	builder.applyOptions(options...)
	return builder
}

func (m *Materializer) Build() (topology.Node, error) {
	s, err := m.Registry.Store(m.Store)
	if err != nil || s == nil {
		return nil, errors.New(`store [` + m.Store + `] dose not exist`)
	}
	m.store = s

	var childs []topology.Node

	for _, childBuilder := range m.childBuilders {
		child, err := childBuilder.Build()
		if err != nil {
			return nil, err
		}

		childs = append(childs, child)
	}
	m.childs = childs

	return m, nil
}

func (m *Materializer) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
	vOut = vIn
	if m.VersionExtractor != nil {
		storeValue, err := m.store.Get(ctx, kIn)
		if err != nil {
			return nil, nil, false, errors.WithPrevious(err, `materializer store read error`)
		}
		newVersion, err := m.VersionExtractor(ctx, kIn, storeValue)
		if err != nil {
			return nil, nil, false, errors.WithPrevious(err, `materializer version extractor error`)
		}
		vOut, err = m.VersionWriter(ctx, newVersion+1, vIn)
		if err != nil {
			return nil, nil, false, errors.WithPrevious(err, `materializer version writer error`)
		}
	}

	err = m.store.Set(ctx, kIn, vOut, 0)
	if err != nil {
		return nil, nil, false, errors.WithPrevious(err, `materializer store write error`)
	}

	for _, child := range m.childs {
		_, _, next, err := child.Run(ctx, kIn, vOut)
		if err != nil || !next {
			return nil, nil, false, err
		}
	}
	return kIn, vOut, true, err
}

func (m *Materializer) ChildBuilders() []topology.NodeBuilder {
	return m.childBuilders
}

func (m *Materializer) AddChildBuilder(builder topology.NodeBuilder) {
	m.childBuilders = append(m.childBuilders, builder)
}

func (m *Materializer) Type() topology.Type {
	return topology.TypeMaterialize
}

func (m *Materializer) Childs() []topology.Node {
	return m.childs
}

func (m *Materializer) AddChild(node topology.Node) {
	m.childs = append(m.childs, node)
}

type MaterializeOption func(sink *Materializer)

func (m *Materializer) applyOptions(options ...MaterializeOption) {
	for _, option := range options {
		option(m)
	}
}

func WithVersionExtractor(ve RecordVersionExtractor) MaterializeOption {
	return func(mat *Materializer) {
		mat.VersionExtractor = ve
	}
}

func WithVersionWriter(vi RecordVersionWriter) MaterializeOption {
	return func(mat *Materializer) {
		mat.VersionWriter = vi
	}
}
