package storageserver

import (
	// "errors"
	"fmt"
	"github.com/cmu440/tribbler/datastructure/nodes"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"sync"
	"time"
)

type storageServer struct {
	ready        bool
	numNodes     int
	selfNode     storagerpc.Node
	nodes        []storagerpc.Node
	rangeChecker func(uint32) bool
	initializer  *nodes.NodesInitializer
	initConf     chan error
	registerLock *sync.Mutex
	chickenRanch *sync.Mutex
	duckRanch    *sync.Mutex
	chicken      map[string]string
	duck         map[string]map[string]bool
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

	tcp, _ := net.ResolveTCPAddr("tcp", "localhost:"+strconv.Itoa(port))
	selfAddr := tcp.String()

	// Setup the HTTP handler that will server incoming RPCs and
	// serve requests in a background goroutine.
	rpc.HandleHTTP()
	go http.Serve(listener, nil)

	storageServer.numNodes = numNodes
	storageServer.selfNode = storagerpc.Node{
		HostPort: selfAddr,
		NodeID:   nodeID,
	}
	storageServer.initializer = nodes.NewNodesInitializer(numNodes)
	ok := storageServer.initializer.Register(storageServer.selfNode)
	storageServer.registerLock = &sync.Mutex{}
	storageServer.initConf = make(chan error, 1)
	storageServer.chickenRanch = &sync.Mutex{}
	storageServer.duckRanch = &sync.Mutex{}
	storageServer.chicken = make(map[string]string)
	storageServer.duck = make(map[string]map[string]bool)

	if masterServerHostPort != "" {
		go storageServer.SlaveInitRoutine(masterServerHostPort)
	} else {
		if ok {
			storageServer.ready = true
			storageServer.nodes = storageServer.initializer.Flush()
			storageServer.rangeChecker = nodes.NewNodeCollection(storageServer.nodes).RangeChecker(storageServer.selfNode.NodeID)
			return storageServer, nil
		}
	}

	err = <-storageServer.initConf
	// if masterServerHostPort == "" {
	// 	for i := 0; i < len(storageServer.nodes); i++ {
	// 		fmt.Println(storageServer.nodes[i].NodeID)
	// 	}
	// 	fmt.Println(len(storageServer.nodes))
	// 	fmt.Println("master server ready")
	// }

	// fmt.Println(storageServer.nodes)

	if err != nil {
		return nil, err
	} else {
		return storageServer, nil
	}
}

func (ss *storageServer) SlaveInitRoutine(masterAddr string) {

	ticker := time.NewTicker(time.Second)
	master, err := rpc.DialHTTP("tcp", masterAddr)
	if err != nil {
		ss.initConf <- err
		return
	}

	args := &storagerpc.RegisterArgs{ServerInfo: ss.selfNode}
	var reply storagerpc.RegisterReply

	for {
		if err := master.Call("StorageServer.RegisterServer", args, &reply); err != nil {
			ss.initConf <- err
			ticker.Stop()
			return
		}

		if reply.Status == storagerpc.OK {

			ss.ready = true
			ss.nodes = reply.Servers
			fmt.Println(ss.selfNode.NodeID)
			ss.rangeChecker = nodes.NewNodeCollection(ss.nodes).RangeChecker(ss.selfNode.NodeID)
			ss.initConf <- nil
			ticker.Stop()
			return
		}

		<-ticker.C
	}

}

// assume will not be called on slaves
func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {

	ss.registerLock.Lock()
	defer ss.registerLock.Unlock()

	ok := ss.initializer.Register(args.ServerInfo)

	if ok {
		ss.ready = true
		ss.nodes = ss.initializer.Flush()
		ss.rangeChecker = nodes.NewNodeCollection(ss.nodes).RangeChecker(ss.selfNode.NodeID)

		*reply = storagerpc.RegisterReply{
			Status:  storagerpc.OK,
			Servers: ss.nodes,
		}
		ss.initConf <- nil
	} else {
		*reply = storagerpc.RegisterReply{
			Status:  storagerpc.NotReady,
			Servers: nil,
		}
	}

	// CAUTION! might have to return error
	return nil

}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	if ss.ready {
		*reply = storagerpc.GetServersReply{
			Status:  storagerpc.OK,
			Servers: ss.nodes,
		}
	} else {
		*reply = storagerpc.GetServersReply{
			Status:  storagerpc.NotReady,
			Servers: nil,
		}
	}
	return nil
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {

	ss.chickenRanch.Lock()
	defer ss.chickenRanch.Unlock()

	hash := libstore.StoreHash(args.Key)
	if rangeOK := ss.rangeChecker(hash); !rangeOK {
		*reply = storagerpc.GetReply{
			Status: storagerpc.WrongServer,
		}
	} else {

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
	}
	return nil
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {

	ss.chickenRanch.Lock()
	defer ss.chickenRanch.Unlock()

	hash := libstore.StoreHash(args.Key)
	if rangeOK := ss.rangeChecker(hash); !rangeOK {
		*reply = storagerpc.DeleteReply{
			Status: storagerpc.WrongServer,
		}
	} else {

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

	hash := libstore.StoreHash(args.Key)
	if rangeOK := ss.rangeChecker(hash); !rangeOK {
		*reply = storagerpc.GetListReply{
			Status: storagerpc.WrongServer,
		}
	} else {

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

	}
	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {

	ss.chickenRanch.Lock()
	defer ss.chickenRanch.Unlock()

	hash := libstore.StoreHash(args.Key)
	if rangeOK := ss.rangeChecker(hash); !rangeOK {
		*reply = storagerpc.PutReply{
			Status: storagerpc.WrongServer,
		}
	} else {

		ss.chicken[args.Key] = args.Value
		*reply = storagerpc.PutReply{
			Status: storagerpc.OK,
		}

	}
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {

	ss.duckRanch.Lock()
	defer ss.duckRanch.Unlock()

	hash := libstore.StoreHash(args.Key)
	if rangeOK := ss.rangeChecker(hash); !rangeOK {
		*reply = storagerpc.PutReply{
			Status: storagerpc.WrongServer,
		}
	} else {

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
	}
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {

	ss.duckRanch.Lock()
	defer ss.duckRanch.Unlock()

	hash := libstore.StoreHash(args.Key)
	if rangeOK := ss.rangeChecker(hash); !rangeOK {
		*reply = storagerpc.PutReply{
			Status: storagerpc.WrongServer,
		}

	} else {

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
	}
	return nil
}
