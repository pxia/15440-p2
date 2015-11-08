package nodes

import (
	// "fmt"
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
		lower = nc.n[i-1].NodeID + 1
	}
	return func(hashValue uint32) bool {
		var hashValueOff uint32
		hashValueOff = hashValue + nc.offset
		return hashValueOff <= upper && lower <= hashValueOff
	}
}
