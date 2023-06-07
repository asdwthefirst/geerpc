package xclient

import (
	"context"
	. "geerpc"
	"io"
	"reflect"
	"sync"
)

type XClient struct {
	d       Discovery
	mode    SelectMode
	opt     *Option
	mu      sync.Mutex
	clients map[string]*Client
}

func (xc XClient) Close() error {
	xc.mu.Lock()
	defer xc.mu.Unlock()
	for k, c := range xc.clients {
		_ = c.Close() //ignore close error
		delete(xc.clients, k)
	}
	return nil
}

func NewXClient(d Discovery, mode SelectMode, opt *Option) *XClient {
	return &XClient{
		d:       d,
		mode:    mode,
		opt:     opt,
		clients: make(map[string]*Client),
	}
}

var _ io.Closer = (*XClient)(nil)

func (xc *XClient) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	rpcAddr, err := xc.d.Get(xc.mode)
	if err != nil {
		return err
	}
	return xc.call(ctx, rpcAddr, serviceMethod, args, reply)
}

func (xc *XClient) call(ctx context.Context, rpcAddr string, serviceMethod string, args interface{}, reply interface{}) error {
	client, err := xc.dial(rpcAddr)
	if err != nil {
		return err
	}
	return client.Call(ctx, serviceMethod, args, reply)
}

func (xc *XClient) dial(rpcAddr string) (*Client, error) {
	xc.mu.Lock()
	defer xc.mu.Unlock()
	client, ok := xc.clients[rpcAddr]
	//被动检查client活跃情况
	if ok && !client.IsAvailable() {
		_ = client.Close()
		delete(xc.clients, rpcAddr)
		client = nil
	}
	if client == nil {
		var err error
		client, err = XDial(rpcAddr, xc.opt)
		if err != nil {
			return nil, err
		}
		xc.clients[rpcAddr] = client
	}
	return client, nil
}

// 发给每一个server实例，有一个出错则返回错误，否则返回其中一个reply
func (xc *XClient) Broadcast(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	rpcAddrs, err := xc.d.GetAll()
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	var mu sync.Mutex //保护e和replyDone和reply
	var e error
	var replyDone bool
	reply = reply == nil //reply为nil时，不需要写返回
	ctx, cancel := context.WithCancel(ctx)
	for _, rpcAddr := range rpcAddrs {
		wg.Add(1)
		go func(ctx context.Context, rpcAddr string) {
			defer wg.Done()
			var clonedReply interface{}
			if reply != nil {
				clonedReply = reflect.New(reflect.TypeOf(reply)).Elem()
			}
			err = xc.call(ctx, rpcAddr, serviceMethod, args, clonedReply) //并发call提高效率，reply涉及写入，使用clonedReply避免数据竞争
			mu.Lock()
			if err != nil && e == nil {
				e = err
				cancel()
			}
			if err == nil && !replyDone {
				reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(clonedReply).Elem())
				replyDone = true
			}
			mu.Unlock()
		}(ctx, rpcAddr)
	}
	wg.Wait()
	return e

}
