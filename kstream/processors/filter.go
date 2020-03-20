package processors

import (
	"context"
	"github.com/tryfix/errors"
	"github.com/tryfix/kstream/kstream/topology"
)

type FilterFunc func(ctx context.Context, key, value interface{}) (bool, error)

type Filter struct {
	Id            int32
	FilterFunc    FilterFunc
	next          bool
	childs        []topology.Node
	childBuilders []topology.NodeBuilder
}

func (f *Filter) ChildBuilders() []topology.NodeBuilder {
	return f.childBuilders
}

func (f *Filter) Childs() []topology.Node {
	return f.childs
}

func (f *Filter) AddChildBuilder(builder topology.NodeBuilder) {
	f.childBuilders = append(f.childBuilders, builder)
}

func (f *Filter) AddChild(node topology.Node) {
	f.childs = append(f.childs, node)
}

func (f *Filter) Build() (topology.Node, error) {
	var childs []topology.Node
	//var childBuilders []node.NodeBuilder

	for _, childBuilder := range f.childBuilders {
		child, err := childBuilder.Build()
		if err != nil {
			return nil, err
		}

		childs = append(childs, child)
	}

	return &Filter{
		FilterFunc: f.FilterFunc,
		childs:     childs,
		next:       f.next,
		Id:         f.Id,
	}, nil
}

func (f *Filter) Name() string {
	return `filter`
}

func (f *Filter) Next() bool {
	return f.next
}

func (f *Filter) Type() topology.Type {
	return topology.Type(`filter`)
}

func (f *Filter) ID() int32 {
	return f.Id
}

func (f *Filter) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, next bool, err error) {

	ok, err := f.FilterFunc(ctx, kIn, vIn)
	if err != nil {
		return nil, nil, false, errors.WithPrevious(err, `process error`)
	}

	if ok {
		for _, child := range f.childs {
			_, _, next, err := child.Run(ctx, kIn, vIn)
			if err != nil || !next {
				return nil, nil, false, err
			}
		}
	}

	return kIn, vIn, ok, nil
}
