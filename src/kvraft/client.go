package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "log"
import "sync"

type Clerk struct {
	mu      sync.Mutex
	servers []*labrpc.ClientEnd

	// You will have to modify this struct.
	reqIdCounter int64 // This shall be a atomic var.
	leaderIndex  int
	me           int64 // UUID
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leaderIndex = -1
	ck.me = nrand() // use a random number
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{Key: key, ReqId: ck.reqIdCounter, ClientId: ck.me}
	ck.reqIdCounter++
	var reply GetReply
	for {
		callingIndex := int(nrand()) % len(ck.servers)
		if ck.leaderIndex != -1 {
			callingIndex = ck.leaderIndex
		}
		ok := ck.servers[callingIndex].Call("RaftKV.Get", &args, &reply)
		if ok {
			if reply.WrongLeader {
				ck.leaderIndex = -1
				// log.Println("Wrong leader")
			} else {
				// record leader
				ck.leaderIndex = callingIndex
			}
			if reply.Err == OK {
				// log.Println("Recv Get ok")
				return reply.Value
			}
		} else {
			log.Println("Network error")
		}
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ReqId:    ck.reqIdCounter,
		ClientId: ck.me}

	ck.reqIdCounter++

	var reply PutAppendReply
	for {
		callingIndex := int(nrand()) % len(ck.servers)
		if ck.leaderIndex != -1 {
			callingIndex = ck.leaderIndex
		}
		ok := ck.servers[callingIndex].Call("RaftKV.PutAppend", &args, &reply)
		if ok {
			if reply.WrongLeader {
				ck.leaderIndex = -1
			} else {
				// record leader
				ck.leaderIndex = callingIndex
			}
			if reply.Err == OK {
				return
			}
		} else {
			log.Println("Network error")
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
