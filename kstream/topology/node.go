package topology

import (
	"context"
)

type Type string

const TypeSource Type = `source`
const TypeSink Type = `sink`
const TypeBranch Type = `branch`
const TypeThrough Type = `through`
const TypeJoiner Type = `joiner`
const TypeMaterialize Type = `materializer`

type Node interface {
	Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error)
	Type() Type
	Childs() []Node
	AddChild(node Node)
}

type NodeBuilder interface {
	Build() (Node, error)
	Type() Type
	ChildBuilders() []NodeBuilder
	AddChildBuilder(builder NodeBuilder)
}
