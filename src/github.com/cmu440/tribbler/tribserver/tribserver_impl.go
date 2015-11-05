package tribserver

import (
	"encoding/json"
	"errors"
	// "fmt"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	"github.com/cmu440/tribbler/util"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"time"
)

type tribServer struct {
	storageServer *rpc.Client
	libStore      libstore.Libstore
}

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {
	storageServer, err := rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		return nil, err
	}
	tribServer := new(tribServer)
	tribServer.storageServer = storageServer
	libStore, err := libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Always)
	if err != nil {
		return nil, err
	}

	tribServer.libStore = libStore

	// Create the server socket that will listen for incoming RPCs.
	listener, err := net.Listen("tcp", myHostPort)
	if err != nil {
		return nil, err
	}

	// Wrap the tribServer before registering it for RPC.
	err = rpc.RegisterName("TribServer", tribrpc.Wrap(tribServer))
	if err != nil {
		return nil, err
	}

	// Setup the HTTP handler that will server incoming RPCs and
	// serve requests in a background goroutine.
	rpc.HandleHTTP()
	go http.Serve(listener, nil)

	return tribServer, nil
	// return nil, errors.New("not implemented")
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	if t, err := ts.userNotFound(args.UserID); err != nil {
		return err
	} else if !t {
		*reply = tribrpc.CreateUserReply{
			Status: tribrpc.Exists,
		}
		return nil
	}

	if err := ts.libStore.Put(util.FormatUserKey(args.UserID), ""); err != nil {
		return err
	}

	*reply = tribrpc.CreateUserReply{
		Status: tribrpc.OK,
	}
	return nil
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	if t, err := ts.userNotFound(args.UserID); err != nil {
		return err
	} else if t {
		*reply = tribrpc.SubscriptionReply{
			Status: tribrpc.NoSuchUser,
		}
		return nil
	}

	if t, err := ts.userNotFound(args.TargetUserID); err != nil {
		return err
	} else if t {
		*reply = tribrpc.SubscriptionReply{
			Status: tribrpc.NoSuchTargetUser,
		}
		return nil
	}

	if err := ts.libStore.AppendToList(util.FormatSubListKey(args.UserID), args.TargetUserID); err == libstore.ItemExistsError {
		*reply = tribrpc.SubscriptionReply{
			Status: tribrpc.Exists,
		}
		return nil
	} else if err != nil {
		return err
	}

	*reply = tribrpc.SubscriptionReply{
		Status: tribrpc.OK,
	}

	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	if t, err := ts.userNotFound(args.UserID); err != nil {
		return err
	} else if t {
		*reply = tribrpc.SubscriptionReply{
			Status: tribrpc.NoSuchUser,
		}
		return nil
	}

	if t, err := ts.userNotFound(args.TargetUserID); err != nil {
		return err
	} else if t {
		*reply = tribrpc.SubscriptionReply{
			Status: tribrpc.NoSuchTargetUser,
		}
		return nil
	}

	if err := ts.libStore.RemoveFromList(util.FormatSubListKey(args.UserID), args.TargetUserID); err == libstore.ItemNotFoundError {
		*reply = tribrpc.SubscriptionReply{
			Status: tribrpc.NoSuchTargetUser,
		}
		return nil
	} else if err != nil {
		return err
	}

	*reply = tribrpc.SubscriptionReply{
		Status: tribrpc.OK,
	}
	return nil
}

func (ts *tribServer) GetSubscriptions(args *tribrpc.GetSubscriptionsArgs, reply *tribrpc.GetSubscriptionsReply) error {
	list, err := ts.libStore.GetList(util.FormatSubListKey(args.UserID))

	if err == libstore.KeyError {
		*reply = tribrpc.GetSubscriptionsReply{
			Status: tribrpc.NoSuchUser,
		}
	} else {
		*reply = tribrpc.GetSubscriptionsReply{
			Status:  tribrpc.OK,
			UserIDs: list,
		}
	}

	return nil
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	if t, err := ts.userNotFound(args.UserID); err != nil {
		// fmt.Println("1")
		return err
	} else if t {
		*reply = tribrpc.PostTribbleReply{
			Status: tribrpc.NoSuchUser,
		}
		return nil
	}

	now := time.Now()
	// var now time.Time
	tribble := tribrpc.Tribble{
		UserID:   args.UserID,
		Posted:   now,
		Contents: args.Contents,
	}
	var tribbleString string
	if buffer, err := json.Marshal(&tribble); err != nil {
		// fmt.Println("2")
		return err
	} else {
		tribbleString = string(buffer)
	}

	// fmt.Println(tribbleString)

	// check post key unique
	// apparent starvation
	postTime := now.UnixNano()
	var postKey string
	for {
		postKey = util.FormatPostKey(args.UserID, postTime)
		if _, err := ts.libStore.Get(postKey); err != nil && err != libstore.KeyError {
			// fmt.Println("3")
			return err
		} else if err == nil {
			// key colliion
			continue
		}

		if err := ts.libStore.Put(postKey, tribbleString); err != nil {
			return err
		}
		break

		// CHECK RESULT RACE?
		// OR NOT?
		// content, err = ts.libStore.Get(postKey)
		// if err != nil {
		// return nil
		// }
		// if content == args.Contents {
		// break
		// }
	}

	if err := ts.libStore.AppendToList(util.FormatTribListKey(args.UserID), postKey); libstore.NotDataError(err) {
		// fmt.Println("4")
		// fmt.Println(err)
		return err
	}

	*reply = tribrpc.PostTribbleReply{
		Status:  tribrpc.OK,
		PostKey: postKey,
	}

	return nil
}

func (ts *tribServer) DeleteTribble(args *tribrpc.DeleteTribbleArgs, reply *tribrpc.DeleteTribbleReply) error {
	if t, err := ts.userNotFound(args.UserID); err != nil {
		return err
	} else if t {
		*reply = tribrpc.DeleteTribbleReply{
			Status: tribrpc.NoSuchUser,
		}
		return nil
	}

	if err := ts.libStore.RemoveFromList(args.UserID, args.PostKey); err == libstore.ItemNotFoundError {
		*reply = tribrpc.DeleteTribbleReply{
			Status: tribrpc.NoSuchPost,
		}
		return nil
	} else if err != nil {
		return err
	}

	if err := ts.libStore.Delete(args.PostKey); libstore.NotDataError(err) {
		return err
	}

	*reply = tribrpc.DeleteTribbleReply{
		Status: tribrpc.OK,
	}

	return nil
}

type Tribbles []tribrpc.Tribble

func (t Tribbles) Len() int {
	return len(t)
}

func (t Tribbles) Less(i, j int) bool {
	return t[i].Posted.After(t[j].Posted)
}

func (t Tribbles) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

func TribblesStringsToObject(tribbleStrings []string) ([]tribrpc.Tribble, error) {
	tribbles := make([]tribrpc.Tribble, len(tribbleStrings))
	i := 0
	for _, tribbleString := range tribbleStrings {
		if err := json.Unmarshal([]byte(tribbleString), &tribbles[i]); err != nil {
			// fmt.Println(tribbleString)
			return nil, err
		}

		i++
	}
	return tribbles, nil
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	if t, err := ts.userNotFound(args.UserID); err != nil {
		return err
	} else if t {
		*reply = tribrpc.GetTribblesReply{
			Status: tribrpc.NoSuchUser,
		}
		return nil
	}

	var postKeys []string
	if list, err := ts.libStore.GetList(util.FormatTribListKey(args.UserID)); err == libstore.KeyError {
		*reply = tribrpc.GetTribblesReply{
			Status:   tribrpc.OK,
			Tribbles: nil,
		}
		return nil
	} else if err != nil {
		return err
	} else {
		// fmt.Println(len(postKeys))
		postKeys = list
	}

	tribbleStrings := make([]string, len(postKeys))
	i := 0
	for _, key := range postKeys {
		if s, err := ts.libStore.Get(key); err != nil {
			return err
		} else {
			tribbleStrings[i] = s
		}
		i++
	}

	var tribbleList Tribbles
	if list, err := TribblesStringsToObject(tribbleStrings); err != nil {
		return err
	} else {
		tribbleList = list
	}

	sort.Sort(tribbleList)
	// fmt.Println(len(tribbleList))
	if len(tribbleList) > 100 {
		*reply = tribrpc.GetTribblesReply{
			Status:   tribrpc.OK,
			Tribbles: tribbleList[:100],
		}
	} else {
		*reply = tribrpc.GetTribblesReply{
			Status:   tribrpc.OK,
			Tribbles: tribbleList,
		}
		// return tribbleList
	}
	return nil
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	return errors.New("not implemented")
}

func (ts *tribServer) userNotFound(UserID string) (bool, error) {
	_, err := ts.libStore.Get(util.FormatUserKey(UserID))
	if err == libstore.KeyError {
		return true, nil
	} else {
		return false, err
	}
}
