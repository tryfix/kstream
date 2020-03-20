package topology

import "context"

type TopologyBuilder struct {
	Source            SourceBuilder
	SourceNodeBuilder NodeBuilder
}

func (tb TopologyBuilder) Build() (Topology, error) {

	topology := Topology{}

	sourceNode, err := tb.SourceNodeBuilder.Build()
	if err != nil {
		return topology, err
	}

	source, err := tb.Source.Build()
	if err != nil {
		return topology, err
	}

	topology.SourceNode = sourceNode
	topology.Source = source

	return topology, nil
}

type Topology struct {
	Source     Source
	SourceNode Node
}

func (t Topology) Run(ctx context.Context, kIn, vIn []byte) (kOut, vOut interface{}, err error) {
	kOut, vOut, err = t.Source.Run(ctx, kIn, vIn)
	if err != nil {
		return nil, nil, err
	}

	_, _, _, err = t.SourceNode.Run(ctx, kOut, vOut)
	if err != nil {
		return nil, nil, err
	}

	return
}
