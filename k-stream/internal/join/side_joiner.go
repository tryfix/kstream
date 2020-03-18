package join

import (
	"context"
	"github.com/tryfix/errors"
	"github.com/tryfix/kstream/k-stream/topology"
)

type SideJoiner struct {
	Id            int32
	Side          string
	LeftWindow    *Window
	RightWindow   *Window
	ValueMapper   ValueMapper
	childs        []topology.Node
	childBuilders []topology.NodeBuilder
}

func (sj *SideJoiner) Build() (topology.Node, error) {
	var childs []topology.Node
	//var childBuilders []node.NodeBuilder

	for _, childBuilder := range sj.childBuilders {
		child, err := childBuilder.Build()
		if err != nil {
			return nil, err
		}

		childs = append(childs, child)
	}

	return &SideJoiner{
		Side:        sj.Side,
		LeftWindow:  sj.LeftWindow,
		RightWindow: sj.RightWindow,
		ValueMapper: sj.ValueMapper,
		childs:      childs,
		Id:          sj.Id,
	}, nil
}

func (sj *SideJoiner) ChildBuilders() []topology.NodeBuilder {
	return sj.childBuilders
}

func (sj *SideJoiner) AddChildBuilder(builder topology.NodeBuilder) {
	sj.childBuilders = append(sj.childBuilders, builder)
}

func (sj *SideJoiner) Next() bool {
	return true
}

func (sj *SideJoiner) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {

	var joinedValue interface{}

	switch sj.Side {
	case `left`:
		v, ok := sj.RightWindow.Read(kIn)
		if !ok {
			sj.LeftWindow.Write(kIn, vIn)
			return nil, nil, false, nil
		}
		joinedValue, err = sj.ValueMapper(vIn, v)
		if err != nil {
			return nil, nil, false, errors.WithPrevious(err,
				`value mapper failed`)
		}
	case `right`:
		v, ok := sj.LeftWindow.Read(kIn)
		if !ok {
			sj.RightWindow.Write(kIn, vIn)
			return nil, nil, false, nil
		}
		joinedValue, err = sj.ValueMapper(v, vIn)
		if err != nil {
			return nil, nil, false, errors.WithPrevious(err,
				`value mapper failed`)
		}
	default:
		return nil, nil, false, errors.New(`stream joiner sides should be only "left" and "right"`)
	}

	for _, child := range sj.childs {
		_, _, _, err := child.Run(ctx, kIn, joinedValue)
		if err != nil {
			return nil, nil, false, err
		}
	}

	return kIn, joinedValue, true, nil
}

func (sj *SideJoiner) Type() topology.Type {
	return topology.Type(sj.Side + `_side_joiner`)
}

func (sj *SideJoiner) Childs() []topology.Node {
	return sj.childs
}

func (sj *SideJoiner) AddChild(node topology.Node) {
	sj.childs = append(sj.childs, node)
}

func (sj *SideJoiner) ID() int32 {
	return sj.Id
}
