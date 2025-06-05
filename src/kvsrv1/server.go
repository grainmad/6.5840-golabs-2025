package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ValueVersion struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	kvmap map[string]ValueVersion
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		kvmap: make(map[string]ValueVersion),
	}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if valueVersion, ok := kv.kvmap[args.Key]; ok {
		reply.Value = valueVersion.Value
		reply.Version = valueVersion.Version
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}
	if Debug {
		DPrintf("Get: %s, value: %s, version: %d, err: %s", args.Key, reply.Value, reply.Version, reply.Err)
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if valueVersion, ok := kv.kvmap[args.Key]; ok { // 存在key
		if args.Version == valueVersion.Version { // 版本匹配
			kv.kvmap[args.Key] = ValueVersion{Value: args.Value, Version: args.Version + 1}
			reply.Err = rpc.OK
		} else { // 版本不匹配
			reply.Err = rpc.ErrVersion
		}
	} else { // 不存在key
		if args.Version == 0 { // 版本为0，安装新值
			kv.kvmap[args.Key] = ValueVersion{Value: args.Value, Version: args.Version + 1}
			reply.Err = rpc.OK
		} else { // 版本不为0，返回错误
			reply.Err = rpc.ErrNoKey
		}
	}
	if Debug {
		DPrintf("Put: %s, value: %s, version: %d, err: %s", args.Key, args.Value, args.Version, reply.Err)
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
