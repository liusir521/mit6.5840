package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	data                map[string]string
	lastclientrequestid map[int64]int64
	lastclientresponse  map[int64]string // 记录每个客户端上次请求的返回值
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.data[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lastReqId, exists := kv.lastclientrequestid[args.ClientId]
	if !exists || args.RequestId > lastReqId {
		kv.lastclientrequestid[args.ClientId] = args.RequestId
		kv.data[args.Key] = args.Value
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lastReqId, exists := kv.lastclientrequestid[args.ClientId]
	if !exists || args.RequestId > lastReqId {
		// 新请求，执行追加
		oldvalue := kv.data[args.Key]
		kv.lastclientrequestid[args.ClientId] = args.RequestId
		kv.data[args.Key] += args.Value
		reply.Value = oldvalue
		// 更新记录值
		kv.lastclientresponse[args.ClientId] = oldvalue
	} else {
		// 重复请求，返回上次记录的响应值
		reply.Value = kv.lastclientresponse[args.ClientId]
	}
}

func StartKVServer() *KVServer {
	kv := &KVServer{
		data:                make(map[string]string),
		lastclientrequestid: make(map[int64]int64),
		lastclientresponse:  make(map[int64]string),
	}

	return kv
}
