package branch

import (
	"context"
	"github.com/tryfix/errors"
	"github.com/tryfix/kstream/kstream/topology"
)

type Predicate func(ctx context.Context, key interface{}, val interface{}) (bool, error)

type Details struct {
	Name      string
	Predicate Predicate
}

type Splitter struct {
	Id             int32
	Branches       []topology.Node
	BranchBuilders []topology.NodeBuilder
}

func (bs *Splitter) ChildBuilders() []topology.NodeBuilder {
	return bs.BranchBuilders
}

func (bs *Splitter) Childs() []topology.Node {
	return bs.Branches
}

func (bs *Splitter) AddChildBuilder(builder topology.NodeBuilder) {
	bs.BranchBuilders = append(bs.BranchBuilders, builder)
}

func (bs *Splitter) AddChild(node topology.Node) {
	bs.Branches = append(bs.Branches, node)
}

func (bs *Splitter) Build() (topology.Node, error) {
	var branches []topology.Node
	//var childBuilders []node.NodeBuilder

	for _, childBuilder := range bs.BranchBuilders {
		branch, err := childBuilder.Build()
		if err != nil {
			return nil, err
		}

		branches = append(branches, branch)
	}

	return &Splitter{
		Branches: branches,
		Id:       bs.Id,
	}, nil
}

func (bs *Splitter) Next() bool {
	return true
}

func (bs *Splitter) ID() int32 {
	return bs.Id
}

func (bs *Splitter) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
	for _, b := range bs.Branches {
		branch, _ := b.(*Branch)

		ok, err := branch.Predicate(ctx, kIn, vIn)
		if err != nil {
			return nil, nil, false, errors.WithPrevious(err, `predicate error`)
		}

		if ok {
			_, _, next, err := branch.Run(ctx, kIn, vIn)
			if err != nil || !next {
				return nil, nil, false, err
			}
			break
		}
	}

	return kIn, kOut, true, nil
}

func (bs *Splitter) Type() topology.Type {
	return topology.Type(`branch_splitter`)
}

type Branch struct {
	Id            int32
	Name          string
	Predicate     Predicate
	childBuilders []topology.NodeBuilder
	childs        []topology.Node
}

func (b *Branch) Childs() []topology.Node {
	return b.childs
}

func (b *Branch) ChildBuilders() []topology.NodeBuilder {
	return b.childBuilders
}

func (b *Branch) AddChildBuilder(builder topology.NodeBuilder) {
	b.childBuilders = append(b.childBuilders, builder)
}

func (b *Branch) AddChild(node topology.Node) {
	b.childs = append(b.childs, node)
}

func (b *Branch) Build() (topology.Node, error) {
	var childs []topology.Node
	//var childBuilders []node.NodeBuilder

	for _, childBuilder := range b.childBuilders {
		child, err := childBuilder.Build()
		if err != nil {
			return nil, err
		}

		childs = append(childs, child)
	}

	return &Branch{
		Name:      b.Name,
		Predicate: b.Predicate,
		childs:    childs,
		Id:        b.Id,
	}, nil
}

func (b *Branch) Next() bool {
	return true
}

func (b *Branch) ID() int32 {
	return b.Id
}

func (b *Branch) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
	for _, child := range b.childs {
		_, _, next, err := child.Run(ctx, kIn, vIn)
		if err != nil || !next {
			return nil, nil, false, err
		}
	}
	return kIn, kOut, true, nil
}

func (b *Branch) Type() topology.Type {
	return topology.TypeBranch
}
