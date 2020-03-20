/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package processors

import (
	"context"
	"github.com/tryfix/errors"
	"github.com/tryfix/kstream/kstream/topology"
)

type ProcessFunc func(ctx context.Context, key, value interface{}) error

type Processor struct {
	Id            int32
	ProcessFunc   ProcessFunc
	childBuilders []topology.NodeBuilder
	childs        []topology.Node
}

func (p *Processor) Childs() []topology.Node {
	return p.childs
}

func (p *Processor) ChildBuilders() []topology.NodeBuilder {
	return p.childBuilders
}

func (p *Processor) AddChildBuilder(builder topology.NodeBuilder) {
	p.childBuilders = append(p.childBuilders, builder)
}

func (p *Processor) AddChild(node topology.Node) {
	p.childs = append(p.childs, node)
}

func (p *Processor) Run(ctx context.Context, kIn, vIn interface{}) (interface{}, interface{}, bool, error) {
	err := p.ProcessFunc(ctx, kIn, vIn)
	if err != nil {
		return kIn, vIn, false, errors.WithPrevious(err, `process error`)
	}

	for _, child := range p.childs {
		_, _, next, err := child.Run(ctx, kIn, vIn)
		if err != nil || !next {
			return nil, nil, false, err
		}
	}

	return kIn, vIn, true, nil
}

func (p *Processor) Build() (topology.Node, error) {
	var childs []topology.Node
	//var childBuilders []node.NodeBuilder

	for _, childBuilder := range p.childBuilders {
		child, err := childBuilder.Build()
		if err != nil {
			return nil, err
		}

		childs = append(childs, child)
	}

	return &Processor{
		ProcessFunc: p.ProcessFunc,
		childs:      childs,
		Id:          p.Id,
	}, nil
}

func (p *Processor) Next() bool {
	return true
}

func (p *Processor) Name() string {
	return `processor`
}

func (p *Processor) Type() topology.Type {
	return topology.Type(`processor`)
}

func (p *Processor) ID() int32 {
	return p.Id
}
