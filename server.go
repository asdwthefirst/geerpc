package geerpc

import (
	"encoding/json"
	"fmt"
	"geerpc/codec"
	"io"
	"log"
	"net"
	"reflect"
	"sync"
)

const MagicNumber = 0x3bef5c

// Option Option采用JSON编码，header和body采用option中的解码方式解码
type Option struct {
	MagicNumber int //标志是geerpc请求
	CodecType   codec.Type
}

var DefaultOption = Option{
	MagicNumber: MagicNumber,
	CodecType:   codec.GobType,
}

// Server Server实例负责监听请求
type Server struct{}

type request struct {
	h            *codec.Header // header of request
	argv, replyv reflect.Value // argv and replyv of request
}

func NewServer() *Server {
	return &Server{}
}

var DefaultServer = NewServer() //方便用户使用

func (server *Server) Accept(lis net.Listener) {
	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Println("rpc server: accept error:", err)
			return
		}
		go server.ServeConn(conn)
	}
}

func Accept(lis net.Listener) {
	DefaultServer.Accept(lis)
}

func (server *Server) ServeConn(conn io.ReadWriteCloser) {
	defer func() { _ = conn.Close() }()
	var opt Option
	//解析geerpc请求
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		log.Println("rpc server: options error: ", err)
		return
	}
	if opt.MagicNumber != MagicNumber {
		log.Printf("rpc server: invalid magic number %x", opt.MagicNumber)
		return
	}
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		log.Printf("rpc server: invalid main type %s", opt.CodecType)
		return
	}
	server.serveCodec(f(conn))
}

// invalidRequest is a placeholder for response argv when error occurs
var invalidRequest = struct{}{}

func (server *Server) serveCodec(cc codec.Codec) {
	wg := &sync.WaitGroup{}  //等待所有请求处理完毕
	sending := &sync.Mutex{} //sendresponse竞争一把锁
	for {
		req, err := server.readRequest(cc)
		if err != nil {
			//todo
			if req == nil {
				break //不用再往后读取的
			}
			req.h.Error = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, sending)
			continue
		}
		wg.Add(1)
		server.handleRequest(cc, req, wg, sending)
	}
	wg.Wait()
	_ = cc.Close()
}

func (server *Server) readRequest(cc codec.Codec) (*request, error) {
	header, err := server.readRequestHeader(cc)
	if err != nil {
		return nil, err
	}
	req := &request{h: header}
	//TODO:解析参数
	req.argv = reflect.New(reflect.TypeOf("")) //接受参数
	if err = cc.ReadBody(req.argv.Interface()); err != nil {
		log.Println("rpc server: read argv err:", err)
	}
	//todo:这里感觉不太合理
	return req, nil

}

func (server *Server) readRequestHeader(cc codec.Codec) (*codec.Header, error) {
	var h codec.Header
	if err := cc.ReadHeader(&h); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Println("rpc server: read header error:", err)
		}
		return nil, err
	}
	return &h, nil
}

func (server *Server) handleRequest(cc codec.Codec, req *request, wg *sync.WaitGroup, sending *sync.Mutex) {
	defer wg.Done()
	log.Println(req.h, req.argv.Elem())
	//TODO:handle的逻辑需要call注册的method处理请求
	req.replyv = reflect.ValueOf(fmt.Sprintf("main resp %d", req.h.Seq))
	server.sendResponse(cc, req.h, req.replyv.Interface(), sending)
}

func (server *Server) sendResponse(cc codec.Codec, header *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := cc.Write(header, body); err != nil {
		log.Println("rpc server: write response error:", err)
	}
}
