package processors

import (
	"context"
	"github.com/tryfix/errors"
	"github.com/tryfix/kstream/kstream/topology"
)

type SelectKeyFunc func(ctx context.Context, key, value interface{}) (kOut interface{}, err error)

type KeySelector struct {
	Id            int32
	SelectKeyFunc SelectKeyFunc
	childBuilders []topology.NodeBuilder
	childs        []topology.Node
}

func (ks *KeySelector) Build() (topology.Node, error) {
	var childs []topology.Node
	//var childBuilders []node.NodeBuilder

	for _, childBuilder := range ks.childBuilders {
		child, err := childBuilder.Build()
		if err != nil {
			return nil, err
		}

		childs = append(childs, child)
	}

	return &KeySelector{
		SelectKeyFunc: ks.SelectKeyFunc,
		childs:        childs,
		Id:            ks.Id,
	}, nil
}

func (ks *KeySelector) ChildBuilders() []topology.NodeBuilder {
	return ks.childBuilders
}

func (ks *KeySelector) AddChildBuilder(builder topology.NodeBuilder) {
	ks.childBuilders = append(ks.childBuilders, builder)
}

func (ks *KeySelector) Next() bool {
	return true
}

func (ks *KeySelector) ID() int32 {
	return ks.Id
}

func (ks *KeySelector) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
	k, err := ks.SelectKeyFunc(ctx, kIn, vIn)
	if err != nil {
		return nil, nil, false, errors.WithPrevious(err, `error in select key function`)
	}

	for _, child := range ks.childs {
		_, _, next, err := child.Run(ctx, k, vIn)
		if err != nil || !next {
			return nil, nil, false, err
		}
	}

	return k, vOut, true, err
}

func (ks *KeySelector) Type() topology.Type {
	return topology.Type(`key_selector`)
}

func (ks *KeySelector) Childs() []topology.Node {
	return ks.childs
}

func (ks *KeySelector) AddChild(node topology.Node) {
	ks.childs = append(ks.childs, node)
}
