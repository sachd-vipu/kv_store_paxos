package kvpaxos

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"net/rpc"
)

type Clerk struct {
	servers []string
	// You will have to modify this struct.
	id      int64
	counter int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.id = nrand()
	ck.counter = 0
	return ck
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.id = nrand()
	getArgs := &GetArgs{Key: key, SeqNo: ck.id}
	var reply GetReply
	ck.counter++
	// forever keep on checking
	for i := 0; ; i = (i + 1) % len(ck.servers) {
		ok := call(ck.servers[i], "KVPaxos.Get", getArgs, &reply)
		if ok {
			if reply.Err == OK {
				return reply.Value
			} else if reply.Err == ErrNoKey { // return "" if the key does not exist
				return ""
			}
		}
	}

}

// shared by Put and Append.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.counter++
	ck.id = nrand()
	putAppendArgs := &PutAppendArgs{Key: key, Value: value, Op: op, SeqNo: ck.id}
	var reply PutAppendReply
	for i := 0; ; i = (i + 1) % len(ck.servers) {
		// simply call the all servers until and return until one of them replies
		ok := call(ck.servers[i], "KVPaxos.PutAppend", putAppendArgs, &reply)
		if ok && reply.Err == OK {
			return
		}

	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
