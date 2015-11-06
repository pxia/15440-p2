package storageserver

import (
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"sync"
)

type storageServer struct {
	numNodes    	int
	joinedNodes 	int
	nodes       	[]storagerpc.Node
	chickenRanch 	*sync.Mutex
	duckRanch    	*sync.Mutex
	chicken     	map[string]string
	duck        	map[string]map[string]bool
}

// NewStorageServer creates and starts a new StorageServer. masterServerHostPort
// is the master storage server's host:port address. If empty, then this server
// is the master; otherwise, this server is a slave. numNodes is the total number of
// servers in the ring. port is the port number that this server should listen on.
// nodeID is a random, unsigned 32-bit ID identifying this server.
//
// This function should return only once all storage servers have joined the ring,
// and should return a non-nil error if the storage server could not be started.
func NewStorageServer(masterServerHostPort string, numNodes, port int, nodeID uint32) (StorageServer, error) {
	storageServer := new(storageServer)

	// Create the server socket that will listen for incoming RPCs.
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		return nil, err
	}

	// Wrap the tribServer before registering it for RPC.
	err = rpc.RegisterName("StorageServer", storagerpc.Wrap(storageServer))
	if err != nil {
		return nil, err
	}

	// Setup the HTTP handler that will server incoming RPCs and
	// serve requests in a background goroutine.
	rpc.HandleHTTP()
	go http.Serve(listener, nil)

	storageServer.numNodes = numNodes
	storageServer.nodes = make([]storagerpc.Node, numNodes)
	storageServer.joinedNodes = 1 // self
	storageServer.chickenRanch = &sync.Mutex{}
	storageServer.duckRanch = &sync.Mutex{}
	storageServer.chicken = make(map[string]string)
	storageServer.duck = make(map[string]map[string]bool)

	return storageServer, nil
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	ss.nodes[ss.joinedNodes] = args.ServerInfo
	ss.joinedNodes++
	if ss.joinedNodes == ss.numNodes {
		*reply = storagerpc.RegisterReply{
			Status:  storagerpc.OK,
			Servers: ss.nodes,
		}
	} else {
		*reply = storagerpc.RegisterReply{
			Status:  storagerpc.NotReady,
			Servers: ss.nodes,
		}
	}
	return nil
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	if ss.joinedNodes == ss.numNodes {
		*reply = storagerpc.GetServersReply{
			Status:  storagerpc.OK,
			Servers: ss.nodes,
		}
	} else {
		*reply = storagerpc.GetServersReply{
			Status:  storagerpc.NotReady,
			Servers: ss.nodes,
		}
	}
	return nil
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {

	ss.chickenRanch.Lock()
	defer ss.chickenRanch.Unlock()

	if v, ok := ss.chicken[args.Key]; !ok {
		*reply = storagerpc.GetReply{
			Status: storagerpc.KeyNotFound,
			Value:  "",
		}
	} else {
		*reply = storagerpc.GetReply{
			Status: storagerpc.OK,
			Value:  v,
		}
	}
	return nil
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {

	ss.chickenRanch.Lock()
	defer ss.chickenRanch.Unlock()

	if _, ok := ss.chicken[args.Key]; !ok {
		*reply = storagerpc.DeleteReply{
			Status: storagerpc.KeyNotFound,
		}
	} else {
		delete(ss.chicken, args.Key)
		*reply = storagerpc.DeleteReply{
			Status: storagerpc.OK,
		}
	}
	return nil
}

func Keys(m map[string]bool) []string {
	keys := make([]string, len(m))

	i := 0
	for k := range m {
		keys[i] = k
		i++
	}
	return keys
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {

	ss.duckRanch.Lock()
	defer ss.duckRanch.Unlock()

	if v, ok := ss.duck[args.Key]; !ok {
		*reply = storagerpc.GetListReply{
			Status: storagerpc.KeyNotFound,
			Value:  nil,
		}
	} else {
		*reply = storagerpc.GetListReply{
			Status: storagerpc.OK,
			Value:  Keys(v),
		}
	}
	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {

	ss.chickenRanch.Lock()
	defer ss.chickenRanch.Unlock()

	ss.chicken[args.Key] = args.Value
	*reply = storagerpc.PutReply{
		Status: storagerpc.OK,
	}
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {

	ss.duckRanch.Lock()
	defer ss.duckRanch.Unlock()

	if _, ok := ss.duck[args.Key]; !ok {
		ss.duck[args.Key] = make(map[string]bool)
	}

	if _, ok := ss.duck[args.Key][args.Value]; ok {
		*reply = storagerpc.PutReply{
			Status: storagerpc.ItemExists,
		}
		return nil
	}

	ss.duck[args.Key][args.Value] = false
	*reply = storagerpc.PutReply{
		Status: storagerpc.OK,
	}
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {

	ss.duckRanch.Lock()
	defer ss.duckRanch.Unlock()

	if _, ok := ss.duck[args.Key]; !ok {
		*reply = storagerpc.PutReply{
			Status: storagerpc.ItemNotFound,
		}
		return nil
	}

	if _, ok := ss.duck[args.Key][args.Value]; !ok {
		*reply = storagerpc.PutReply{
			Status: storagerpc.ItemNotFound,
		}
		return nil
	}

	delete(ss.duck[args.Key], args.Value)
	*reply = storagerpc.PutReply{
		Status: storagerpc.OK,
	}
	return nil
}
