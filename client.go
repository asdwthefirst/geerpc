package geerpc

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"geerpc/codec"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

type Call struct {
	Seq           uint64
	ServiceMethod string //format "<service>.<method>"
	Args          interface{}
	Reply         interface{}
	Err           error
	Done          chan *Call
	//Done对外暴露监听完成情况
	//Done使用*Call作为chan结构感觉意义不大
}

// done()不对外暴露
func (call *Call) done() {
	call.Done <- call
}

type Client struct {
	cc       codec.Codec
	opt      *Option
	sending  sync.Mutex // 发送锁
	header   codec.Header
	mu       sync.Mutex // protect following
	seq      uint64
	pending  map[uint64]*Call
	closing  bool // 主动关闭
	shutdown bool // 被动关闭
}

var ErrShutdown = errors.New("connection is shut down")

func (c Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing {
		return ErrShutdown
	}
	c.closing = true
	return c.cc.Close()
}

var _ io.Closer = (*Client)(nil)

func (c *Client) IsAvailable() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.shutdown && !c.closing
}

func (c *Client) registerCall(call *Call) (seq uint64, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	//todo:why check?
	if c.closing || c.shutdown {
		return 0, ErrShutdown
	}
	call.Seq = c.seq
	c.seq++
	c.pending[call.Seq] = call
	return call.Seq, nil
}

func (c *Client) removeCall(seq uint64) *Call {
	c.mu.Lock()
	defer c.mu.Unlock()
	call := c.pending[seq]
	delete(c.pending, seq)
	return call
}

// 服务端或客户端发生错误时调用，将 shutdown 设置为 true，且将错误信息通知所有 pending 状态的 call。
func (c *Client) terminateCalls(err error) {
	c.sending.Lock()
	defer c.sending.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.shutdown = true
	for _, call := range c.pending {
		call.Err = err
		call.done()
	}
}

func (c *Client) receive() {
	var err error
	for err == nil {
		var h codec.Header
		if err = c.cc.ReadHeader(&h); err != nil {
			break
		}
		call := c.removeCall(h.Seq)
		switch {
		case call == nil:
			// it usually means that Write partially failed
			// and call was already removed.
			log.Println("rpc client reveive empty call")
			err = c.cc.ReadBody(nil)
		case h.Error != "":
			log.Println("rpc client reveive h.Error!=nil")
			call.Err = fmt.Errorf(h.Error)
			err = c.cc.ReadBody(nil)
			call.done()
		default:
			err = c.cc.ReadBody(call.Reply)
			if err != nil {
				log.Println("rpc client reveive readbody err:", err)
				call.Err = errors.New("reading body " + err.Error())
			}
			call.done()
		}
	}
	// error occurs, so terminateCalls pending calls
	c.terminateCalls(err)
}

func (c *Client) send(call *Call) (err error) {
	c.sending.Lock()
	defer c.sending.Unlock()
	call.Seq, err = c.registerCall(call)
	if err != nil {
		log.Println("rpc client send register err:", err)
		call.Err = err
		call.done()
		return err
	}
	//todo:我不理解为什么client要管理一个header，按理来说一个call一个call发送，应该不会有单独发送header的情况，
	//todo:那么client管理的header是什么？当前发送的callheader？
	//c.header.ServiceMethod = call.ServiceMethod
	//c.header.Seq = call.Seq
	//c.header.Error = ""

	err = c.cc.Write(&codec.Header{ServiceMethod: call.ServiceMethod, Seq: call.Seq}, call.Args)
	if err != nil {
		log.Println("rpc client send write err:", err)
		call.Err = err
		call.done()
		return err
	}
	return err

}

func NewClient(conn net.Conn, opt *Option) (*Client, error) {
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		err := fmt.Errorf("invalid codec type %s", opt.CodecType)
		log.Println("rpc client: codec error:", err)
		return nil, err
	}
	// 先发送opt
	if err := json.NewEncoder(conn).Encode(opt); err != nil {
		log.Println("rpc client: options error: ", err)
		_ = conn.Close()
		return nil, err
	}
	client := &Client{
		seq:     1, // seq starts with 1, 0 means invalid call
		cc:      f(conn),
		opt:     opt,
		pending: make(map[uint64]*Call),
	}
	go client.receive()
	return client, nil
}

func NewHTTPClient(conn net.Conn, opt *Option) (*Client, error) {
	_, _ = io.WriteString(conn, fmt.Sprintf("CONNECT %s HTTP/1.0\n\n", defaultRPCPath))
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
	if err == nil && resp.Status == connected {
		return NewClient(conn, opt)
	}
	if err == nil {
		err = errors.New("unexpected HTTP response: " + resp.Status)
	}
	return nil, err
}

type clientResult struct {
	client *Client
	err    error
}

type newClientFunc func(conn net.Conn, opt *Option) (client *Client, err error)

func Dial(network, address string, opts ...*Option) (client *Client, err error) {
	return dialTimeout(NewClient, network, address, opts...)
}

func DialHTTP(network, address string, opts ...*Option) (*Client, error) {
	return dialTimeout(NewHTTPClient, network, address, opts...)
}

func dialTimeout(f newClientFunc, network, address string, opts ...*Option) (client *Client, err error) {
	opt, err := parseOption(opts...)
	if err != nil {
		return nil, err
	}
	//conn, err := net.Dial(network, address)
	conn, err := net.DialTimeout(network, address, opt.ConnectTimeout)
	if err != nil {
		return nil, err
	}
	// close the connection if client is nil
	defer func() {
		if err != nil {
			_ = conn.Close()
		}
	}()

	ch := make(chan clientResult)

	go func() {
		client, err = f(conn, opt)
		ch <- clientResult{client: client, err: err}
	}()

	if opt.ConnectTimeout == 0 {
		cliRes := <-ch
		return cliRes.client, cliRes.err
	}
	select {
	case cliRes := <-ch:
		return cliRes.client, cliRes.err
	case <-time.After(opt.ConnectTimeout):
		return nil, fmt.Errorf("rpc client: connect timeout: expect within %s", opt.ConnectTimeout)
	}

	return client, err
}

func parseOption(opts ...*Option) (*Option, error) {
	//没传opt用默认opt
	if len(opts) == 0 || opts[0] == nil {
		return DefaultOption, nil
	}
	if len(opts) != 1 {
		return nil, errors.New("number of options is more than 1")
	}
	opt := opts[0]
	//todo:MagicNumber既然用作识别为什么不是判断语句而是指定
	opt.MagicNumber = DefaultOption.MagicNumber
	if opt.CodecType == "" {
		opt.CodecType = DefaultOption.CodecType
	}
	return opt, nil
}

// Go done可以传nil，不能是无缓冲chan
func (c *Client) Go(serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	//todo:为什么不能使用unbuffered，只是不阻塞call的返回吗？可是call就是同步调用，为什么不能阻塞？
	//todo:而且我不理解为什么要传done，会有什么需要传入的情况吗，直接make好像更合适。
	if done == nil {
		done = make(chan *Call, 10)
	} else if cap(done) == 0 {
		log.Panic("rpc client: done channel is unbuffered")
	}
	call := &Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Done:          done,
	}
	c.send(call)
	return call

}

// ctx, _ := context.WithTimeout(context.Background(), time.Second)
// err := client.Call(ctx, "Foo.Sum", &Args{1, 2}, &reply)
func (c *Client) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	call := c.Go(serviceMethod, args, reply, make(chan *Call, 1))
	select {
	case <-ctx.Done():
		c.removeCall(call.Seq)
		return errors.New("rpc client: call failed: ctx Done: " + ctx.Err().Error())
	case call = <-call.Done:
		return call.Err
	}
}
