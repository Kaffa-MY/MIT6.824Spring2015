package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk

	// Your declarations here.
	view viewservice.View  // current view
	kv   map[string]string // key-value store
	seq  map[int64]int64   // id->seq of client
}

func (pb *PBServer) isPrimary() bool {
	return pb.me == pb.view.Primary
}

func (pb *PBServer) isBackup() bool {
	return pb.me == pb.view.Backup
}

func (pb *PBServer) hasBackup() bool {
	return pb.view.Backup != ""
}

func (pb *PBServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	if args.Seq > pb.seq[args.ClientId] {
		pb.kv[args.Key] = args.Value
		pb.seq[args.ClientId] = args.Seq
	}
	reply.Err = OK
}

func (pb *PBServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	if pb.seq[args.ClientId] < args.Seq {
		prev, exist := pb.kv[args.Key]
		if !exist {
			prev = ""
		}
		pb.kv[args.Key] = prev + args.Value
		pb.seq[args.ClientId] = args.Seq
	}
	reply.Err = OK
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	//fmt.Printf("Server Get arg=%v\n", *args)

	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.isBackup() && args.Forwarded {
		reply.Err = OK
	} else if pb.isBackup() && !args.Forwarded {
		reply.Err = ErrWrongServer
	} else if pb.isPrimary() && args.Forwarded {
		reply.Err = ErrWrongServer
	} else if pb.isPrimary() && !args.Forwarded {
		value, exist := pb.kv[args.Key]
		if !exist {
			reply.Value = ""
		} else {
			reply.Value = value
		}
		reply.Err = OK

		if pb.hasBackup() {
			forwardArg := &GetArgs{args.Key, args.ClientId, args.Seq, true}
			forwardReply := &GetReply{}
			ok := call(pb.view.Backup, "PBServer.Get", forwardArg, &forwardReply)
			if !ok || (forwardReply.Err == ErrWrongServer) {
				reply.Err = ErrWrongServer
				return nil
			}
		}
	}
	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	//fmt.Printf("Server PutAppend args=%v\n", *args)
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.isPrimary() && args.Forwarded {
		reply.Err = ErrWrongServer

	} else if pb.isPrimary() && !args.Forwarded {
		switch args.Operation {
		case OpPut:
			pb.Put(args, reply)
		case OpAppend:
			pb.Append(args, reply)
		}

		if pb.hasBackup() {
			forwardArgs := &PutAppendArgs{args.Key, args.Value, args.Operation, args.ClientId, args.Seq, true}
			forwardReply := &PutAppendReply{}
			ok := call(pb.view.Backup, "PBServer.PutAppend", forwardArgs, &forwardReply)
			if !ok || (forwardReply.Err == ErrWrongServer) {
				reply.Err = ErrWrongServer
				return nil
			}
		}

	} else if pb.isBackup() && args.Forwarded {
		switch args.Operation {
		case OpPut:
			pb.Put(args, reply)

		case OpAppend:
			pb.Append(args, reply)
		}
	} else if pb.isBackup() && !args.Forwarded {
		reply.Err = ErrWrongServer
	}

	return nil
	////////////////////////old implementation/////////////////////////////////////

	// var forwardArg ForwardArg
	// var forwardReply FordwardReply

	// forwardArg.Operation = args.Operation
	// forwardArg.Key = args.Key
	// forwardArg.Value = args.Value
	// forwardArg.ClientId = args.ClientId
	// forwardArg.Seq = args.Seq

	// if !pb.isPrimary() {
	// 	reply.Err = ErrWrongServer
	// 	return nil
	// }

	// reply.Err = OK
	// switch args.Operation {
	// case OpPut:
	// 	if pb.seq[args.ClientId] < args.Seq {
	// 		pb.kv[args.Key] = args.Value
	// 	} else {
	// 		reply.Err = OK
	// 		return nil
	// 	}
	// case OpAppend:
	// 	if pb.seq[args.ClientId] < args.Seq {
	// 		prev, exist := pb.kv[args.Key]
	// 		if !exist {
	// 			prev = ""
	// 		}
	// 		pb.kv[args.Key] = prev + args.Value
	// 		pb.seq[args.ClientId] = args.Seq
	// 	} else {
	// 		reply.Err = OK
	// 		return nil
	// 	}
	// }

	// if pb.hasBackup() {
	// 	ok := call(pb.view.Backup, "PBServer.Forward", forwardArg, &forwardReply)
	// 	if !ok || (forwardReply.Err == ErrWrongServer) {
	// 		reply.Err = ErrWrongServer
	// 		return nil
	// 	}
	// }

	//fmt.Printf("Server PutAppend result: %v\n", *reply)
}

// It turns out the primary must send Gets as well as Puts to the backup (if there is one),
// and must wait for the backup to reply before responding to the client.
// This helps prevent two servers from acting as primary (a "split brain")
// func (pb *PBServer) Forward(args *ForwardArg, reply *FordwardReply) error {
// 	pb.mu.Lock()
// 	defer pb.mu.Unlock()

// 	if !pb.isBackup() {
// 		reply.Err = ErrWrongServer
// 		return nil
// 	}
// 	op := args.Operation
// 	switch op {
// 	case OpGet:
// 		reply.Err = OK
// 	case OpPut:
// 		reply.Err = OK
// 		if pb.seq[args.ClientId] < args.Seq {
// 			pb.kv[args.Key] = args.Value
// 		}
// 	case OpAppend:
// 		reply.Err = OK
// 		if pb.seq[args.ClientId] < args.Seq {
// 			prev, exist := pb.kv[args.Key]
// 			if !exist {
// 				prev = ""
// 			}
// 			pb.kv[args.Key] = prev + args.Value
// 			pb.seq[args.ClientId] = args.Seq
// 		}
// 	}
// 	return nil
// }

func (pb *PBServer) Sync(arg *SyncArgs, reply *SyncReply) error {
	//fmt.Printf("Sync ... \n")
	pb.mu.Lock()
	defer pb.mu.Unlock()

	v, _ := pb.vs.Ping(pb.view.Viewnum)

	// update view of backup
	if v.Viewnum != pb.view.Viewnum {
		pb.view = v
	}

	// check the current backup
	if pb.isBackup() {
		pb.kv = arg.KV
		pb.seq = arg.Seq
		reply.Err = OK
	} else {
		reply.Err = ErrWrongServer
	}
	//fmt.Printf("Sync done!\n")
	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	view, _ := pb.vs.Ping(pb.view.Viewnum)

	if pb.view != view {
		pb.view = view
		// sync between primary and backup
		if pb.isPrimary() && pb.hasBackup() {
			args := &SyncArgs{pb.kv, pb.seq}
			reply := &SyncReply{}
			call(pb.view.Backup, "PBServer.Sync", args, &reply)
		}
	}

}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	//fmt.Printf("Start Server now ...\n")
	pb.kv = make(map[string]string)
	pb.seq = make(map[int64]int64)
	pb.view = viewservice.View{0, "", ""}
	//fmt.Printf("initial PBServer is %v\n", pb)
	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
