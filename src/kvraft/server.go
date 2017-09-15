package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string // optional if it's Get
	Op       string
	ReqId    int64 // Used for notification of invocation response
	ClientId int64
}

type Requests map[int64]chan Op

func GetOrAdd(reqs *map[int64]Requests, clientId int64) *Requests {
	req, ok := (*reqs)[clientId]
	if ok {
		return &req
	} else {
		(*reqs)[clientId] = make(map[int64]chan Op)
		ret := (*reqs)[clientId]
		return &ret
	}
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kv map[string]string // The actual kv store

	clientRequests map[int64]Requests // handler will wait for the Op to come
}

// Return true if current raft server is the leader.
func (kv *RaftKV) TryWriteOp(op *Op, reqId int64, clientId int64) (bool, chan Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// Start is no blocking.
	_, _, isLeader := kv.rf.Start(*op)
	var ret_chan chan Op
	if isLeader {
		requests := GetOrAdd(&kv.clientRequests, clientId)
		(*requests)[reqId] = make(chan Op)

		ret_chan = (*requests)[reqId]
	}
	return isLeader, ret_chan
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// kv.mu.Lock()
	// defer kv.mu.Unlock()
	// log.Println("Me: ", kv.me, " recv Get: ", args)

	isLeader, req_chan :=
		kv.TryWriteOp(
			&Op{
				Key:      args.Key,
				Op:       "Get",
				ReqId:    args.ReqId,
				ClientId: args.ClientId},
			args.ReqId,
			args.ClientId)

	if !isLeader {
		reply.WrongLeader = true
		return
	}
	op := <-req_chan

	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, ok := kv.kv[op.Key]
	if ok {
		reply.Err = OK
		reply.Value = value
	} else {
		reply.Err = ErrNoKey
	}
	reply.WrongLeader = false
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// log.Println("Me: ", kv.me, " recv PutAppend: ", args)
	isLeader, req_chan :=
		kv.TryWriteOp(
			&Op{
				Key:      args.Key,
				Value:    args.Value,
				Op:       args.Op,
				ReqId:    args.ReqId,
				ClientId: args.ClientId},
			args.ReqId,
			args.ClientId)

	if !isLeader {
		reply.WrongLeader = true
		return
	}
	// must not hold a lock when waiting.
	<-req_chan
	reply.Err = OK
	reply.WrongLeader = false
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *RaftKV) Apply() {
	// Read from the applyCh.
	for {
		select {
		case msg := <-kv.applyCh:
			// if there exists a command.ReqId in the map.
			op := msg.Command.(Op)
			kv.mu.Lock()
			if op.Op == "Put" {
				kv.kv[op.Key] = op.Value
			}
			if op.Op == "Append" {
				kv.kv[op.Key] += op.Value
			}
			// do nothing when it's Get.
			requests := GetOrAdd(&kv.clientRequests, op.ClientId)
			request, ok := (*requests)[op.ReqId]
			kv.mu.Unlock()
			if ok {
				request <- op
			}
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.clientRequests = make(map[int64]Requests)
	kv.kv = make(map[string]string)

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	go kv.Apply()
	return kv
}
