package storageserver

import (
	// "errors"
	// "fmt"
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

type valEntry struct {
	v         string
	revoking  bool
	writeLock *sync.Mutex
	readLock  *sync.Mutex
}

type listEntry struct {
	l         map[string]bool
	revoking  bool
	writeLock *sync.Mutex
	readLock  *sync.Mutex
}

type storageServer struct {
	ready        bool
	numNodes     int
	selfNode     storagerpc.Node
	nodes        []storagerpc.Node
	rangeChecker func(uint32) bool
	initializer  *nodes.NodesInitializer
	initConfChan chan error
	registerLock *sync.Mutex
	valLock      *sync.Mutex
	listLock     *sync.Mutex
	valTable     map[string]valEntry
	listTable    map[string]listEntry
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
	storageServer.initConfChan = make(chan error, 1)
	storageServer.valLock = &sync.Mutex{}
	storageServer.listLock = &sync.Mutex{}
	storageServer.valTable = make(map[string]valEntry)
	storageServer.listTable = make(map[string]listEntry)

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

	err = <-storageServer.initConfChan

	if err != nil {
		return nil, err
	} else {
		storageServer.ready = true
		return storageServer, nil
	}
}

func (ss *storageServer) SlaveInitRoutine(masterAddr string) {

	ticker := time.NewTicker(time.Second)
	master, err := rpc.DialHTTP("tcp", masterAddr)
	if err != nil {
		ss.initConfChan <- err
		return
	}

	args := &storagerpc.RegisterArgs{ServerInfo: ss.selfNode}
	var reply storagerpc.RegisterReply

	for {
		if err := master.Call("StorageServer.RegisterServer", args, &reply); err != nil {
			// fmt.Println("warning!")
			ss.initConfChan <- err
			ticker.Stop()
			return
		}

		if reply.Status == storagerpc.OK {

			ss.ready = true
			ss.nodes = reply.Servers
			ss.rangeChecker = nodes.NewNodeCollection(ss.nodes).RangeChecker(ss.selfNode.NodeID)
			ss.initConfChan <- nil
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
		ss.nodes = ss.initializer.Flush()
		ss.rangeChecker = nodes.NewNodeCollection(ss.nodes).RangeChecker(ss.selfNode.NodeID)

		*reply = storagerpc.RegisterReply{
			Status:  storagerpc.OK,
			Servers: ss.nodes,
		}
		if !ss.ready {
			ss.initConfChan <- nil
		}
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

	hash := libstore.StoreHash(args.Key)
	if rangeOK := ss.rangeChecker(hash); !rangeOK {
		*reply = storagerpc.GetReply{
			Status: storagerpc.WrongServer,
		}
	} else {

		ss.valLock.Lock()
		v, ok := ss.valTable[args.Key]
		ss.valLock.Unlock()

		if !ok {

			reply.Status = storagerpc.KeyNotFound

		} else {

			v.readLock.Lock()

			reply.Status = storagerpc.OK
			reply.Value = v.v

			// TODO: check if grant lease
			if args.WantLease && (!v.revoking) {
				reply.Lease.Granted = true
				reply.Lease.ValidSeconds = storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds
			}

			v.readLock.Unlock()

		}
	}
	return nil
}

// TODO: check for conflicts & race, need to revoke!
func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {

	ss.valLock.Lock()
	defer ss.valLock.Unlock()

	hash := libstore.StoreHash(args.Key)
	if rangeOK := ss.rangeChecker(hash); !rangeOK {
		*reply = storagerpc.DeleteReply{
			Status: storagerpc.WrongServer,
		}
	} else {

		if _, ok := ss.valTable[args.Key]; !ok {
			*reply = storagerpc.DeleteReply{
				Status: storagerpc.KeyNotFound,
			}
		} else {
			delete(ss.valTable, args.Key)
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

	hash := libstore.StoreHash(args.Key)
	if rangeOK := ss.rangeChecker(hash); !rangeOK {
		*reply = storagerpc.GetListReply{
			Status: storagerpc.WrongServer,
		}
	} else {

		ss.listLock.Lock()
		v, ok := ss.listTable[args.Key]
		ss.listLock.Unlock()

		if !ok {
			*reply = storagerpc.GetListReply{
				Status: storagerpc.KeyNotFound,
				Value:  nil,
			}
		} else {

			v.readLock.Lock()

			reply.Status = storagerpc.OK
			reply.Value = Keys(v.l)

			// TODO: check if grant lease
			if args.WantLease && (!v.revoking) {
				reply.Lease.Granted = true
				reply.Lease.ValidSeconds = storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds
			}

			v.readLock.Unlock()

		}

	}
	return nil
}

// TODO: what if the key exists in the pool?
func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {

	hash := libstore.StoreHash(args.Key)
	if rangeOK := ss.rangeChecker(hash); !rangeOK {
		*reply = storagerpc.PutReply{
			Status: storagerpc.WrongServer,
		}
	} else {

		ss.valLock.Lock()
		defer ss.valLock.Unlock()

		ss.valTable[args.Key] = valEntry{
			v:         args.Value,
			revoking:  false,
			readLock:  &sync.Mutex{},
			writeLock: &sync.Mutex{},
		}
		*reply = storagerpc.PutReply{
			Status: storagerpc.OK,
		}

	}
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {

	hash := libstore.StoreHash(args.Key)
	if rangeOK := ss.rangeChecker(hash); !rangeOK {
		*reply = storagerpc.PutReply{
			Status: storagerpc.WrongServer,
		}
	} else {

		ss.listLock.Lock()
		entry, ok := ss.listTable[args.Key]
		if !ok {
			entry = listEntry{
				l:         make(map[string]bool),
				revoking:  false,
				readLock:  &sync.Mutex{},
				writeLock: &sync.Mutex{},
			}
			ss.listTable[args.Key] = entry
		}
		ss.listLock.Unlock()

		entry.writeLock.Lock()
		entry.readLock.Lock()
		defer entry.readLock.Unlock()
		defer entry.writeLock.Unlock()

		if _, ok := ss.listTable[args.Key].l[args.Value]; ok {
			*reply = storagerpc.PutReply{
				Status: storagerpc.ItemExists,
			}
			return nil
		}

		ss.listTable[args.Key].l[args.Value] = false
		*reply = storagerpc.PutReply{
			Status: storagerpc.OK,
		}

	}
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {

	hash := libstore.StoreHash(args.Key)
	if rangeOK := ss.rangeChecker(hash); !rangeOK {
		*reply = storagerpc.PutReply{
			Status: storagerpc.WrongServer,
		}

	} else {

		ss.listLock.Lock()
		entry, ok := ss.listTable[args.Key]
		ss.listLock.Unlock()

		if !ok {
			*reply = storagerpc.PutReply{
				Status: storagerpc.ItemNotFound,
			}
			return nil
		}

		entry.writeLock.Lock()
		entry.readLock.Lock()
		defer entry.readLock.Unlock()
		defer entry.writeLock.Unlock()

		if _, ok := ss.listTable[args.Key].l[args.Value]; !ok {
			*reply = storagerpc.PutReply{
				Status: storagerpc.ItemNotFound,
			}
			return nil
		}

		delete(ss.listTable[args.Key].l, args.Value)
		*reply = storagerpc.PutReply{
			Status: storagerpc.OK,
		}

	}
	return nil
}
