package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
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

type Status int

const (
	SUCCESS Status = 1 + iota
	WRONG_LEADER
	DUPILCATE
)

// Return true if current raft server is the leader.
func (kv *RaftKV) TryWriteOp(op *Op, reqId int64, clientId int64) (Status, chan Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var ret_chan chan Op
	// Check if there already has an entry with reqId and clientId.
	reqs, ok := kv.clientRequests[clientId]
	if ok {
		req, ok := reqs[reqId]
		if ok && req == nil {
			// ignore
			log.Println("Me: ", kv.me, " client duplicate packet")
			return DUPILCATE, ret_chan
		} else if ok {
			log.Println("Me: ", kv.me, " mysterious leader fault, req come back, do it again")
			// return SUCCESS, ret_chan // Try to wait again.
		}
	}
	// Start is no blocking.
	_, _, isLeader := kv.rf.Start(*op)
	if isLeader {
		requests := GetOrAdd(&kv.clientRequests, clientId)
		(*requests)[reqId] = make(chan Op)

		ret_chan = (*requests)[reqId]
		return SUCCESS, ret_chan
	}
	return WRONG_LEADER, ret_chan
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// kv.mu.Lock()
	// defer kv.mu.Unlock()
	// log.Println("Me: ", kv.me, " recv Get: ", args)

	status, req_chan :=
		kv.TryWriteOp(
			&Op{
				Key:      args.Key,
				Op:       "Get",
				ReqId:    args.ReqId,
				ClientId: args.ClientId},
			args.ReqId,
			args.ClientId)
	if status == DUPILCATE {
		reply.WrongLeader = false
		reply.Err = OK

		kv.mu.Lock()
		defer kv.mu.Unlock()
		reply.Value = kv.kv[args.Key]
		return
	}
	if status == WRONG_LEADER {
		reply.WrongLeader = true
		return
	}
	var op Op
	select {
	case op = <-req_chan:
		break
	case <-time.After(1000 * time.Millisecond):
		// Get timeout can be two scenarios:
		// 1. it's not a leader anymore.
		// 2. it's too slow.

		// _, isLeader := kv.rf.GetState()
		// // if term is different
		// if isLeader {
		// 	log.Fatal("Me: ", kv.me, "Being a leader and timeout!!")
		// }
		log.Println("Me: ", kv.me, " recv Get: ", args, " timeout")
		// kv.mu.Lock()
		// defer kv.mu.Unlock()
		// requests := GetOrAdd(&kv.clientRequests, args.ClientId)
		// delete(*requests, args.ReqId)
		reply.WrongLeader = true
		return
	}

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
	status, req_chan :=
		kv.TryWriteOp(
			&Op{
				Key:      args.Key,
				Value:    args.Value,
				Op:       args.Op,
				ReqId:    args.ReqId,
				ClientId: args.ClientId},
			args.ReqId,
			args.ClientId)
	if status == DUPILCATE {
		reply.WrongLeader = false
		reply.Err = OK
		return
	}
	if status == WRONG_LEADER {
		reply.WrongLeader = true
		return
	}
	// must not hold a lock when waiting.
	select {
	case <-req_chan:
		break
	case <-time.After(1000 * time.Millisecond):
		log.Println("Me: ", kv.me, " recv Put: ", args, " timeout")
		// remove the entry if fail b/c of timeout.
		// kv.mu.Lock()
		// defer kv.mu.Unlock()
		// requests := GetOrAdd(&kv.clientRequests, args.ClientId)
		// delete(*requests, args.ReqId)

		reply.WrongLeader = true
		return
	}
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
			// Detect change of term here:
			op := msg.Command.(Op)
			kv.mu.Lock()
			// if there is already a req entry being applied before. Don't apply.
			requests := GetOrAdd(&kv.clientRequests, op.ClientId)
			request, ok := (*requests)[op.ReqId]
			if ok && request == nil {
				// do no update.
				log.Println("Me: ", kv.me, " already replied before.")
				kv.mu.Unlock()
				continue
			}
			if op.Op == "Put" {
				kv.kv[op.Key] = op.Value
			}
			if op.Op == "Append" {
				kv.kv[op.Key] += op.Value
			}
			kv.mu.Unlock()
			if ok {
				// log.Println("Me: ", kv.me, " apply with req: ", op)
				request <- op
			}
			kv.mu.Lock()
			// nil means successfully respond to a client.
			kv.clientRequests[op.ClientId][op.ReqId] = nil // insert a fake entry
			kv.mu.Unlock()
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
