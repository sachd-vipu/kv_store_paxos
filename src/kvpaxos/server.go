package kvpaxos

import (
	"cse-513/src/paxos"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
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

	Operation string
	Key       string
	Value     string
	SeqNo     int64
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.

	store map[string]string
	seq   int // seq for next req
	log   map[int64]bool
}

func (kv *KVPaxos) runOperation(op *Op) {
	if op.Operation == Put {
		kv.store[op.Key] = op.Value
	} else if op.Operation == Append {
		kv.store[op.Key] += op.Value
	} else {
		// do nothing
	}
	// update the current history log
	kv.log[op.SeqNo] = true
	return
}

func (kv *KVPaxos) beginDecisionPhase(currOperation Op) (Err, string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if result, contains := kv.checkOperationsInLog(currOperation); contains {
		return result.err, result.value
	}

	for {
		if result, completed := kv.checkPaxosDecision(currOperation); completed {
			return result.err, result.value
		}
	}
}

func (kv *KVPaxos) checkOperationsInLog(currOperation Op) (resultDecision, bool) {
	if _, contains := kv.log[currOperation.SeqNo]; contains {
		if currOperation.Operation == Get {
			return resultDecision{OK, kv.store[currOperation.Key]}, true
		} else {
			return resultDecision{OK, ""}, true
		}
	}
	return resultDecision{}, false
}

func (kv *KVPaxos) checkPaxosDecision(currOperation Op) (resultDecision, bool) {
	sleepInterval := 8 * time.Millisecond
	kv.px.Start(kv.seq, currOperation)
	timeOut := 0 * time.Millisecond

	for {
		fate, value := kv.px.Status(kv.seq)
		if fate == paxos.Decided {
			return kv.handleDecidedCase(value.(Op), currOperation), true
		} else if fate == paxos.Pending {
			if timeOut > 8*time.Second {
				return resultDecision{ErrPending, ""}, true
			}
			time.Sleep(sleepInterval)
			timeOut += sleepInterval
			sleepInterval *= 3
		} else { // covers cases other than Decided and Pending
			return resultDecision{ErrForgotten, ""}, true
		}
	}
}

func (kv *KVPaxos) handleDecidedCase(decidedOperation Op, originalOperation Op) resultDecision {
	kv.px.Done(kv.seq)
	kv.runOperation(&decidedOperation)
	kv.seq++

	if decidedOperation.SeqNo == originalOperation.SeqNo {
		if decidedOperation.Operation == Get {
			if value, ok := kv.store[originalOperation.Key]; ok {
				return resultDecision{OK, value}
			}
			return resultDecision{ErrNoKey, ""}
		}
		return resultDecision{OK, ""}
	}
	return resultDecision{}
}

type resultDecision struct {
	err   Err
	value string
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	op := Op{Operation: Get, Key: args.Key, Value: "", SeqNo: args.SeqNo}
	reply.Err, reply.Value = kv.beginDecisionPhase(op)
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	op := Op{Operation: args.Op, Key: args.Key, Value: args.Value, SeqNo: args.SeqNo}
	reply.Err, _ = kv.beginDecisionPhase(op)
	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.

	kv.seq = 0
	kv.store = make(map[string]string)
	kv.log = make(map[int64]bool)

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
