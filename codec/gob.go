package codec

import (
	"bufio"
	"encoding/gob"
	"io"
	"log"
)

type GobCodec struct {
	conn io.ReadWriteCloser
	buf  *bufio.Writer
	dec  *gob.Decoder
	enc  *gob.Encoder
}

func NewGobCodec(conn io.ReadWriteCloser) Codec {
	return &GobCodec{
		conn: conn,
		buf:  bufio.NewWriter(conn), //improve the performance of I/O operations by reducing the number of system calls
		enc:  gob.NewEncoder(conn),
		dec:  gob.NewDecoder(conn),
	}
}

func (g *GobCodec) Close() error { //释放资源
	var err error
	if err = g.buf.Flush(); err != nil {
		return err
	}
	if err = g.conn.Close(); err != nil {
		return err
	}
	g.dec = nil //解除引用方便垃圾回收
	g.enc = nil
	g.buf = nil
	g.conn = nil
	return nil
}

func (g GobCodec) ReadHeader(header *Header) error {
	return g.dec.Decode(header)
}

func (g GobCodec) ReadBody(body interface{}) error {
	return g.dec.Decode(body)
}

func (g GobCodec) Write(header *Header, body interface{}) (err error) {
	defer func() {
		_ = g.buf.Flush()
		if err != nil {
			_ = g.Close()
		}
	}()
	if err = g.enc.Encode(header); err != nil {
		log.Println("rpc main: gob error encoding header:", err)
		return err
	}
	if err = g.enc.Encode(body); err != nil {
		log.Println("rpc main: gob error encoding body:", err)
		return err
	}
	return nil

}
