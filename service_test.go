package geerpc

import (
	"fmt"
	"reflect"
	"testing"
)

type Foo int

type Args struct{ Num1, Num2 int }

func (f Foo) Sum(args Args, reply *int) error {
	*reply = args.Num1 + args.Num2
	return nil
}

// it's not a exported methods
func (f Foo) sum(args Args, reply *int) error {
	*reply = args.Num1 + args.Num2
	return nil
}

func _assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+msg, v...))
	}
}

func TestNewService(t *testing.T) {
	var foo Foo
	s := newService(&foo)
	_assert(len(s.methods) == 1, "wrong service methods, expect 1, but got %d", len(s.methods))
	mType := s.methods["Sum"]
	_assert(mType != nil, "wrong methods, Sum shouldn't nil")
}

func TestMethodType_Call(t *testing.T) {
	var foo Foo
	s := newService(&foo)
	//fmt.Println(s.methods)
	mType := s.methods["Sum"]

	argv := mType.newArgv()
	replyv := mType.newReplyv()
	argv.Set(reflect.ValueOf(Args{Num1: 1, Num2: 3}))
	//argv.Elem().Set(reflect.ValueOf(Args{Num1: 1, Num2: 3}))如果argv为指针类型，那就要拿出elem。
	err := s.call(mType, argv, replyv)
	_assert(err == nil && *replyv.Interface().(*int) == 4 && mType.NumCalls() == 1, "failed to call Foo.Sum")
}
