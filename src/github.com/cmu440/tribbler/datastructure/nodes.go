package main

import (
	"fmt"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"math"
	"sort"
)

type NodesInitializer struct {
	l int
	// NodeID --> Node
	n map[uint32]storagerpc.Node
}

type NodeCollection struct {
	offset uint32
	n      []storagerpc.Node
}

func NewNodesInitializer(numNodes int) *NodesInitializer {
	return &NodesInitializer{
		l: numNodes,
		n: make(map[uint32]storagerpc.Node),
	}
}

func (ni *NodesInitializer) Register(node storagerpc.Node) bool {
	ni.n[node.NodeID] = node
	return len(ni.n) == ni.l
}

// naming it just to sort...
type nodeSlice []storagerpc.Node

func (n nodeSlice) Len() int {
	return len(n)
}

func (n nodeSlice) Less(i, j int) bool {
	return n[i].NodeID < n[j].NodeID
}

func (n nodeSlice) Swap(i, j int) {
	n[i], n[j] = n[j], n[i]
}

func (ni *NodesInitializer) Flush() []storagerpc.Node {
	nodes := make([]storagerpc.Node, len(ni.n))
	i := 0
	for _, node := range ni.n {
		nodes[i] = node
		i++
	}
	return nodes
	// nodes := make(nodeSlice, len(ni.n))
	// sort.Sort(nodes)
	// var offset uint32
	// offset = math.MaxUint32 - nodes[len(nodes)-1].NodeID
	// for _, node := range nodes {
	// 	node.NodeID += offset
	// }
	// return &Nodes{
	// 	offset: offset,
	// 	n:      nodes,
	// }
}

func NewNodeCollection(nodes []storagerpc.Node) *NodeCollection {
	var nodesSorted = nodeSlice(nodes)
	sort.Sort(nodesSorted)
	var offset uint32
	offset = math.MaxUint32 - nodesSorted[len(nodesSorted)-1].NodeID
	for i, _ := range nodesSorted {
		nodesSorted[i].NodeID += offset
	}
	return &NodeCollection{
		offset: offset,
		n:      nodesSorted,
	}
}

func (nc *NodeCollection) Route(hashValue uint32) *storagerpc.Node {
	var hashValueOff uint32
	hashValueOff = hashValue + nc.offset
	i := sort.Search(len(nc.n), func(i int) bool { return hashValueOff <= nc.n[i].NodeID })
	return &nc.n[i]
}

func (nc *NodeCollection) RangeChecker(NodeID uint32) func(uint32) bool {
	var NodeIDOff uint32
	NodeIDOff = NodeID + nc.offset
	i := sort.Search(len(nc.n), func(i int) bool { return NodeIDOff <= nc.n[i].NodeID })
	upper := nc.n[i].NodeID
	var lower uint32
	if i == 0 {
		lower = 0
	} else {
		lower = nc.n[i-1].NodeID
	}
	return func(hashValue uint32) bool {
		var hashValueOff uint32
		hashValueOff = hashValue + nc.offset
		return hashValueOff <= upper && lower < hashValueOff
	}
}

func main() {
	nc := NewNodeCollection([]storagerpc.Node{
		{NodeID: 5, HostPort: "a"},
		{NodeID: 10, HostPort: "b"},
		{NodeID: 15, HostPort: "c"},
	})
	checker1 := nc.RangeChecker(5)
	fmt.Println(checker1(4))
	fmt.Println(checker1(5))
	fmt.Println(checker1(6))
	fmt.Println(checker1(10))
	checker2 := nc.RangeChecker(10)
	fmt.Println(checker2(9))
	fmt.Println(checker2(10))
	fmt.Println(checker2(11))
}
