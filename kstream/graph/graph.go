package graph

import (
	"fmt"
	"github.com/awalterschulze/gographviz"
	"github.com/tryfix/kstream/kstream/branch"
	"github.com/tryfix/kstream/kstream/internal/join"
	"github.com/tryfix/kstream/kstream/processors"
	"github.com/tryfix/kstream/kstream/store"
	"github.com/tryfix/kstream/kstream/topology"
	"strings"
)

type Graph struct {
	parent   string
	vizGraph *gographviz.Graph
}

func NewGraph() *Graph {
	parent := `root`
	g := gographviz.NewGraph()
	if err := g.SetName(parent); err != nil {
		panic(err)
	}
	if err := g.SetDir(true); err != nil {
		panic(err)
	}

	if err := g.AddAttr(parent, `splines`, `ortho`); err != nil {
		panic(err)
	}

	if err := g.AddAttr(parent, `size`, `"50,12"`); err != nil {
		panic(err)
	}

	if err := g.AddNode(parent, `kstreams`, map[string]string{
		`fontcolor`: `grey100`,
		`fillcolor`: `limegreen`,
		`style`:     `filled`,
		`label`:     `"KStreams"`,
	}); err != nil {
		panic(err)
	}

	if err := g.AddNode(parent, `def`, map[string]string{
		`shape`: `plaintext`,
		`label`: `<
     		<table BORDER="0" CELLBORDER="1" CELLSPACING="0">
       			<tr><td WIDTH="50" BGCOLOR="slateblue4"></td> <td><B>Processor Node</B></td></tr>
       			<tr><td WIDTH="50" BGCOLOR="deepskyblue1"></td><td><B>Global Table</B></td></tr>
       			<tr><td WIDTH="50" BGCOLOR="grey95"></td><td><B>Store Backend</B></td></tr>
       			<tr><td WIDTH="50" BGCOLOR="black"></td><td><B>Stream Branch</B></td></tr>
       			<tr><td WIDTH="50" BGCOLOR="limegreen"></td><td><B>Predicate</B></td></tr>
       			<tr><td WIDTH="50" BGCOLOR="deepskyblue1"></td><td><B>Source</B></td></tr>
       			<tr><td WIDTH="50" BGCOLOR="orange"></td><td><B>Sink</B></td></tr>
     		</table>

  >`,
	}); err != nil {
		panic(err)
	}

	if err := g.AddNode(`kstreams`, `streams`, nil); err != nil {
		panic(err)
	}

	if err := g.AddEdge(`kstreams`, `streams`, true, nil); err != nil {
		panic(err)
	}

	return &Graph{
		parent:   parent,
		vizGraph: g,
	}
}

func (g *Graph) Root(parent string, name string, attrs map[string]string, edgeAttrs map[string]string) {
	if err := g.vizGraph.AddSubGraph(g.parent, name, attrs); err != nil {
		panic(err)
	}
}

func (g *Graph) SubGraph(parent string, name string, attrs map[string]string, edgeAttrs map[string]string) {
	if err := g.vizGraph.AddNode(parent, name, attrs); err != nil {
		panic(err)
	}
}

func (g *Graph) Source(parent string, name string, attrs map[string]string, edgeAttrs map[string]string) {
	attrs[`color`] = `black`
	attrs[`fillcolor`] = `deepskyblue1`
	attrs[`style`] = `filled`
	attrs[`shape`] = `oval`
	if err := g.vizGraph.AddNode(parent, name, attrs); err != nil {
		panic(err)
	}

	if err := g.vizGraph.AddEdge(parent, name, true, edgeAttrs); err != nil {
		panic(err)
	}
}

func (g *Graph) GTableStreams(parent string, name string, attrs map[string]string, edgeAttrs map[string]string) {
	attrs[`color`] = `black`
	attrs[`fillcolor`] = `deepskyblue1`
	attrs[`style`] = `filled`
	attrs[`shape`] = `oval`
	if err := g.vizGraph.AddNode(parent, name, attrs); err != nil {
		panic(err)
	}

	if err := g.vizGraph.AddEdge(parent, name, true, edgeAttrs); err != nil {
		panic(err)
	}
}

func (g *Graph) Processor(parent string, name string, attrs map[string]string, edgeAttrs map[string]string) {
	attrs[`fontcolor`] = `grey100`
	attrs[`fillcolor`] = `slateblue4`
	attrs[`style`] = `filled`
	if err := g.vizGraph.AddNode(g.parent, name, attrs); err != nil {
		panic(err)
	}

	if parent != `` {
		if err := g.vizGraph.AddEdge(parent, name, true, edgeAttrs); err != nil {
			panic(err)
		}
	}

}

func (g *Graph) Predicate(parent string, name string, attrs map[string]string, edgeAttrs map[string]string) {
	attrs[`fontcolor`] = `black`
	attrs[`fillcolor`] = `olivedrab2`
	attrs[`shape`] = `rectangle`
	attrs[`style`] = `"rounded,filled"`
	if err := g.vizGraph.AddNode(g.parent, name, attrs); err != nil {
		panic(err)
	}

	if parent != `` {
		if err := g.vizGraph.AddEdge(parent, name, true, edgeAttrs); err != nil {
			panic(err)
		}
	}

}

func (g *Graph) Joiner(parent string, name string, store string, attrs map[string]string, edgeAttrs map[string]string) {
	//attrs[`color`] = `black`
	attrs[`shape`] = `plaintext`
	attrs[`fontsize`] = `11`
	//attrs[`style`] = `filled`
	if err := g.vizGraph.AddNode(g.parent, name, attrs); err != nil {
		panic(err)
	}

	if err := g.vizGraph.AddEdge(store, name, true, nil); err != nil {
		panic(err)
	}

	if parent != `` {
		if err := g.vizGraph.AddEdge(parent, name, true, edgeAttrs); err != nil {
			panic(err)
		}
	}

}

func (g *Graph) StreamJoiner(leftParent string, name string, RightParent string, attrs map[string]string, edgeAttrs map[string]string) {
	attrs[`color`] = `brown`
	attrs[`shape`] = `square`
	attrs[`fontsize`] = `11`
	attrs[`style`] = `filled`
	if err := g.vizGraph.AddNode(g.parent, name, attrs); err != nil {
		panic(err)
	}

	//if err := g.vizGraph.AddEdge(store, name, true, nil); err != nil {
	//	panic(err)
	//}

	if leftParent != `` {
		if err := g.vizGraph.AddEdge(leftParent, name, true, edgeAttrs); err != nil {
			panic(err)
		}
	}

}

func (g *Graph) Edge(parent string, name string, attrs map[string]string, edgeAttrs map[string]string) {
	if err := g.vizGraph.AddEdge(parent, name, true, attrs); err != nil {
		panic(err)
	}
}

func (g *Graph) Branch(parent string, name string, async bool, order int, attrs map[string]string, edgeAttrs map[string]string) {
	attrs[`fontcolor`] = `grey100`
	attrs[`fillcolor`] = `accent3`
	attrs[`fontname`] = `Arial`
	attrs[`fontsize`] = `14`
	attrs[`shape`] = `rectangle`
	attrs[`style`] = `"rounded,filled"`
	if err := g.vizGraph.AddNode(g.parent, name, attrs); err != nil {
		panic(err)
	}

	if parent != `` {
		adgeAttrs := map[string]string{
			`style`: `dashed`,
		}
		adgeAttrs[`label`] = fmt.Sprintf(`< <B>%d</B> >`, order)
		if async {
			adgeAttrs[`label`] = `< <B>ASYNC</B> >`
		}

		if err := g.vizGraph.AddEdge(parent, name, true, adgeAttrs); err != nil {
			panic(err)
		}
	}
}

func (g *Graph) Sink(parent string, name string, attrs map[string]string, edgeAttrs map[string]string) {
	attrs[`color`] = `black`
	attrs[`fillcolor`] = `orange`
	attrs[`style`] = `filled`
	attrs[`shape`] = `oval`
	if err := g.vizGraph.AddNode(g.parent, name, attrs); err != nil {
		panic(err)
	}

	if parent != `` {
		if err := g.vizGraph.AddEdge(parent, name, true, edgeAttrs); err != nil {
			panic(err)
		}
	}
}

func (g *Graph) Store(parent string, store store.Store, attrs map[string]string, edgeAttrs map[string]string) {
	attrs[`shape`] = `cylinder`
	attrs[`fillcolor`] = `grey95`
	attrs[`style`] = `filled`
	if err := g.vizGraph.AddNode(g.parent, store.Name(), attrs); err != nil {
		panic(err)
	}

	if parent != `` {
		if err := g.vizGraph.AddEdge(parent, store.Name(), true, edgeAttrs); err != nil {
			panic(err)
		}
	}
}

func (g *Graph) RenderTopology(t *topology.TopologyBuilder) {

	nName := `streams_` + fmt.Sprint(getId())
	sourceName := strings.ReplaceAll(t.Source.Name(), `-`, `_`)
	sourceName = strings.ReplaceAll(sourceName, `.`, `_`)
	g.Source(`streams`, nName, map[string]string{
		`label`: fmt.Sprintf(`"%s"`, nodeInfo(t.Source.SourceType(), sourceName, t.Source.Info())),
		`shape`: `square`,
	}, nil)

	draw(nName, t.SourceNodeBuilder.ChildBuilders(), g)
}

func (g *Graph) Build() string {
	return g.vizGraph.String()
}

func draw(parent string, builders []topology.NodeBuilder, graph *Graph) {

	var nodeName string
	for id, b := range builders {
		edgeAttr := map[string]string{
			`label`: fmt.Sprintf(`"%d"`, id+1),
		}
		switch n := b.(type) {
		case *processors.Processor:
			nName := n.Name() + fmt.Sprint(n.ID())
			nodeName = nName
			graph.Processor(parent, nName, map[string]string{
				//`label`: fmt.Sprintf(`"%s"`, n.Name()),
				`label`: `"PRO"`,
				`shape`: `square`,
			}, edgeAttr)

		case topology.SinkBuilder:
			//nName := parent + fmt.Sprint(n.ID())
			nName := n.Name() + string(n.Type()) + fmt.Sprint(n.ID())
			nName = strings.ReplaceAll(nName, `.`, `_`)
			nName = strings.ReplaceAll(nName, `-`, `_`)
			nodeName = nName
			graph.Sink(parent, nName, map[string]string{
				`label`: fmt.Sprintf(`"%s"`, nodeInfo(n.SinkType(), n.Name(), n.Info())),
				`shape`: `square`,
			}, edgeAttr)

		case *branch.Branch:
			nName := string(n.Type()) + fmt.Sprint(n.ID())
			nodeName = nName
			graph.Predicate(parent, nName, map[string]string{
				`label`: fmt.Sprintf(`"  P \n   %s  "`, n.Name),
			}, edgeAttr)

		case *join.GlobalTableJoiner:
			nName := string(n.Type()) + fmt.Sprint(n.ID())
			nodeName = nName
			var typ = `INNER`
			if n.Typ == join.LeftJoin {
				typ = `LEFT`
			}
			//println(`xxxxxxxxxxxxxx`)
			graph.Joiner(parent, nName, n.Store, map[string]string{
				`label`: fmt.Sprintf(`< <B>%s</B> >`, typ+` JOIN`),
			}, edgeAttr)

		case *processors.Filter:
			nName := n.Name() + fmt.Sprint(n.ID())
			nodeName = nName
			graph.Processor(parent, nName, map[string]string{
				`label`: `"F"`,
				`shape`: `square`,
			}, edgeAttr)

		case *processors.Transformer:
			nName := n.Name() + fmt.Sprint(n.ID())
			nodeName = nName
			graph.Processor(parent, nName, map[string]string{
				`label`: `"T"`,
				`shape`: `square`,
			}, edgeAttr)

		case *branch.Splitter:
			i := n.ID()
			nName := string(n.Type()) + fmt.Sprint(i)
			nodeName = nName

			graph.Branch(parent, nName, false, int(i), map[string]string{
				`label`: fmt.Sprintf(`"%s"`, n.Type()),
			}, edgeAttr)

		case *processors.KeySelector:
			nName := string(n.Type()) + fmt.Sprint(n.ID())
			nodeName = nName
			graph.Processor(parent, nName, map[string]string{
				`label`: `"KS"`,
				`shape`: `square`,
			}, edgeAttr)
		case *processors.ValueTransformer:
			nName := string(n.Type()) + fmt.Sprint(n.ID())
			nodeName = nName
			graph.Processor(parent, nName, map[string]string{
				`label`: `"TV"`,
				`shape`: `square`,
			}, edgeAttr)
		case *join.SideJoiner:
			nName := string(n.Type()) + fmt.Sprint(n.ID())
			nodeName = nName

			graph.StreamJoiner(parent, nName, ``, map[string]string{
				`label`: fmt.Sprintf(`< <B>%s</B> >`, string(n.Type())+`_JOIN`),
			}, edgeAttr)
		case *join.StreamJoiner:
			nName := string(n.Type()) + fmt.Sprint(n.ID())
			nodeName = nName

			graph.StreamJoiner(parent, nName, ``, map[string]string{
				`label`: fmt.Sprintf(`< <B>%s</B> >`, string(n.Type())+`_JOIN`),
			}, edgeAttr)
		}

		draw(nodeName, b.ChildBuilders(), graph)
	}
}

var id int

func getId() int {
	id += 1
	return id
}

func nodeInfo(typ string, name string, info map[string]string) string {
	str := fmt.Sprintf(`type:%s\nname:%s\n`, typ, name)
	for p, v := range info {
		str += fmt.Sprintf(`%s:%s \n`, p, v)
	}

	return str
}
