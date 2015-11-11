package conns

import (
	"net/rpc"
	"sync"
)

type RpcPool struct {
	m map[string]*rpc.Client
	l *sync.Mutex
}

func NewRPCPool() *RpcPool {
	return &RpcPool{
		m: make(map[string]*rpc.Client),
		l: &sync.Mutex{},
	}
}

func (r *RpcPool) Add(hostport string, cli *rpc.Client) {
	r.l.Lock()
	defer r.l.Unlock()
	r.m[hostport] = cli
}

func (r *RpcPool) Try(hostport string) *rpc.Client {
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
func (r *RpcPool) Delete(hostport string) {
	r.l.Lock()
	defer r.l.Unlock()
	delete(r.m, hostport)
}
