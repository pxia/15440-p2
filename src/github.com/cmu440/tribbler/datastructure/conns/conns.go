package conns

import (
	"net/rpc"
	"sync"
)

type rpcPool struct {
	m map[string]*rpc.Client
	l *sync.Mutex
}

func NewRPCPool() *rpcPool {
	return &rpcPool{
		m: make(map[string]*rpc.Client),
		l: &sync.Mutex{},
	}
}

func (r *rpcPool) Add(hostport string, cli *rpc.Client) {
	r.l.Lock()
	defer r.l.Unlock()
	r.m[hostport] = cli
}

func (r *rpcPool) Try(hostport string) *rpc.Client {
	r.l.Lock()
	defer r.l.Unlock()
	v, ok := r.m[hostport]
	if ok {
		return v
	}
	if cli, err := rpc.DialHTTP("tcp", hostport); err != nil {
		return nil
	} else {
		r.m[hostport] = cli
		return cli
	}
}

// not sure when to use this
func (r *rpcPool) Delete(hostport string) {
	r.l.Lock()
	defer r.l.Unlock()
	delete(r.m, hostport)
}
