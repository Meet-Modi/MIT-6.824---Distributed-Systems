package kvsrv

import (
	"fmt"
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
	store             map[string]string
	clientRequests    map[string]bool
	muClientsRequests sync.Mutex
}

type ClientRequestId struct {
	ClientId  uint64
	RequestId uint64
}

func (kv *KVServer) IsDuplicateRequest(clientId uint64, requestId uint64) bool {
	kv.muClientsRequests.Lock()
	key := fmt.Sprintf("%d-%d", clientId, requestId)
	_, ok := kv.clientRequests[key]
	if !ok {
		kv.clientRequests[key] = true
	}
	kv.muClientsRequests.Unlock()
	return ok
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.IsDuplicateRequest(args.ClientId, args.RequestId) {
		// fmt.Println("cId : ", args.ClientId, " duplicate request!!, key: ", args.Key)
		return
	}
	// fmt.Println("cId : ", args.ClientId, " reqId : ", args.RequestId, " key: ", args.Key)

	kv.mu.Lock()
	if oldVal, ok := kv.store[args.Key]; ok {
		reply.Value = oldVal
	} else {
		reply.Value = ""
	}
	kv.mu.Unlock()
	fmt.Println("Get ::: client ", args.ClientId, " ", args.RequestId, " ", args.Key, " ", kv.store[args.Key], "  ", reply.Value)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.IsDuplicateRequest(args.ClientId, args.RequestId) {
		return
	}

	kv.mu.Lock()
	oldVal, ok := kv.store[args.Key]
	if ok {
		reply.Value = oldVal
	} else {
		reply.Value = ""
	}
	kv.store[args.Key] = args.Value
	fmt.Println("+++++++++++++++++++ PUT client: ", args.ClientId, " key : ", args.Key, " oldVal: ", oldVal, " newVal: ", args.Value, " reply: ", reply.Value)

	kv.mu.Unlock()
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.IsDuplicateRequest(args.ClientId, args.RequestId) {
		return
	}

	kv.mu.Lock()
	oldVal, ok := kv.store[args.Key]

	var newVal string
	if ok {
		newVal = oldVal
	} else {
		newVal = ""
	}
	newVal += args.Value

	reply.Value = oldVal

	kv.store[args.Key] = newVal

	fmt.Println("+++++++++++++++++++ APPEND client: ", args.ClientId, " key : ", args.Key, " oldVal: ", oldVal, " newVal: ", newVal, " reply: ", reply.Value)
	kv.mu.Unlock()
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.store = make(map[string]string)
	kv.clientRequests = make(map[string]bool)

	// putArgs := &PutAppendArgs{
	// 	Key:   "meetKey",
	// 	Value: "meetValue",
	// }
	// putReply := &PutAppendReply{
	// 	Value: "",
	// }
	// getArgs := &GetArgs{
	// 	Key: "meetKey",
	// }
	// getReply := &GetReply{
	// 	Value: "",
	// }
	// kv.Put(putArgs, putReply)
	// fmt.Println("Put completed")
	// kv.Get(getArgs, getReply)
	// fmt.Println("Get completed")
	// fmt.Println(getArgs, getReply)
	return kv
}
