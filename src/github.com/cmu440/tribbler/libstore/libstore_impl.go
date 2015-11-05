package libstore

import (
	"errors"
	"net/rpc"

	"github.com/cmu440/tribbler/rpc/librpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
)

var (
	KeyError          = errors.New("Key Error")
	ItemExistsError   = errors.New("Item exists")
	ItemNotFoundError = errors.New("Item not found")
	// these will cause panic!
	RoutingError  = errors.New("Routing Error")
	ProtocolError = errors.New("Protocol Error")
)

type libstore struct {
	storageServer *rpc.Client
	myHostPort    string
}

// NewLibstore creates a new instance of a TribServer's libstore. masterServerHostPort
// is the master storage server's host:port. myHostPort is this Libstore's host:port
// (i.e. the callback address that the storage servers should use to send back
// notifications when leases are revoked).
//
// The mode argument is a debugging flag that determines how the Libstore should
// request/handle leases. If mode is Never, then the Libstore should never request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to false). If mode is Always, then the Libstore should always request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to true). If mode is Normal, then the Libstore should make its own
// decisions on whether or not a lease should be requested from the storage server,
// based on the requirements specified in the project PDF handout.  Note that the
// value of the mode flag may also determine whether or not the Libstore should
// register to receive RPCs from the storage servers.
//
// To register the Libstore to receive RPCs from the storage servers, the following
// line of code should suffice:
//
//     rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
//
// Note that unlike in the NewTribServer and NewStorageServer functions, there is no
// need to create a brand new HTTP handler to serve the requests (the Libstore may
// simply reuse the TribServer's HTTP handler since the two run in the same process).
func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode) (Libstore, error) {

	cli, err := rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		return nil, err
	}
	libstore := new(libstore)
	rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
	libstore.storageServer = cli
	libstore.myHostPort = myHostPort

	return libstore, nil
}

func (ls *libstore) Get(key string) (string, error) {
	args := &storagerpc.GetArgs{
		Key:       key,
		WantLease: false,
		HostPort:  ls.myHostPort,
	}
	var reply storagerpc.GetReply

	if err := ls.storageServer.Call("StorageServer.Get", args, &reply); err != nil {
		return "", err
	}

	if reply.Status != storagerpc.OK {
		return "", KeyError
	}

	switch reply.Status {
	case storagerpc.OK:
		return reply.Value, nil
	case storagerpc.KeyNotFound:
		return "", KeyError
	case storagerpc.WrongServer:
		return "", RoutingError
	default:
		return "", ProtocolError
	}

}

func (ls *libstore) Put(key, value string) error {
	args := &storagerpc.PutArgs{
		Key:   key,
		Value: value,
	}
	var reply storagerpc.PutReply

	if err := ls.storageServer.Call("StorageServer.Put", args, &reply); err != nil {
		return err
	}

	switch reply.Status {
	case storagerpc.OK:
		return nil
	case storagerpc.WrongServer:
		return RoutingError
	default:
		return ProtocolError
	}
}

func (ls *libstore) Delete(key string) error {
	args := &storagerpc.DeleteArgs{
		Key: key,
	}
	var reply storagerpc.DeleteReply

	if err := ls.storageServer.Call("StorageServer.Delete", args, &reply); err != nil {
		return err
	}

	switch reply.Status {
	case storagerpc.OK:
		return nil
	case storagerpc.WrongServer:
		return RoutingError
	case storagerpc.KeyNotFound:
		return KeyError
	default:
		return ProtocolError
	}

	return nil
}

func (ls *libstore) GetList(key string) ([]string, error) {
	args := &storagerpc.GetArgs{
		Key:       key,
		WantLease: false,
		HostPort:  ls.myHostPort,
	}
	var reply storagerpc.GetListReply

	if err := ls.storageServer.Call("StorageServer.GetList", args, &reply); err != nil {
		return nil, err
	}

	switch reply.Status {
	case storagerpc.OK:
		return reply.Value, nil
	case storagerpc.WrongServer:
		return nil, RoutingError
	case storagerpc.KeyNotFound:
		return nil, KeyError
	default:
		return nil, ProtocolError
	}

}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	args := &storagerpc.PutArgs{
		Key:   key,
		Value: removeItem,
	}
	var reply storagerpc.PutReply

	if err := ls.storageServer.Call("StorageServer.RemoveFromList", args, &reply); err != nil {
		return err
	}

	switch reply.Status {
	case storagerpc.OK:
		return nil
	case storagerpc.WrongServer:
		return RoutingError
	case storagerpc.ItemNotFound:
		return ItemNotFoundError
	default:
		return ProtocolError
	}
}

func (ls *libstore) AppendToList(key, newItem string) error {
	args := &storagerpc.PutArgs{
		Key:   key,
		Value: newItem,
	}
	var reply storagerpc.PutReply

	if err := ls.storageServer.Call("StorageServer.AppendToList", args, &reply); err != nil {
		return err
	}

	switch reply.Status {
	case storagerpc.OK:
		return nil
	case storagerpc.WrongServer:
		return RoutingError
	case storagerpc.ItemExists:
		return ItemExistsError
	default:
		return ProtocolError
	}
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	return errors.New("not implemented")
}

func NotDataError(e error) bool {
	return e != nil && e != KeyError && e != ItemExistsError && e != ItemNotFoundError
}
