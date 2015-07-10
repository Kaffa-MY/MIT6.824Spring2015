package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	timeStamp map[string]time.Time // keep track of the most recent time at which the viewservice has heard a Ping from each server
	view      *View                // keep track of the current view
	viewAcked bool
}

// update view when some server has changed
func (vs *ViewServer) updateView(primary string, backup string) bool {
	if vs.view.Primary != primary || vs.view.Backup != backup {
		vs.view = &View{vs.view.Viewnum + 1, primary, backup}
		vs.viewAcked = false
		return true
	}
	return false
}

// remove some server when it is dead
func (vs *ViewServer) removeServer(server string) bool {
	switch server {
	case vs.view.Primary:
		if vs.viewAcked && vs.view.Backup != "" {
			vs.updateView(vs.view.Backup, "")
		}

	case vs.view.Backup:
		vs.updateView(vs.view.Primary, "")
	}
	return true
}

// add an idle server when it is needed
func (vs *ViewServer) addServer(server string) bool {
	if !vs.viewAcked {
		return false
	} else {
		if vs.view.Primary == "" {
			return vs.updateView(server, "")
		} else if vs.view.Backup == "" && vs.view.Primary != server {
			return vs.updateView(vs.view.Primary, server)
		}
		return false
	}
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	server, viewNum := args.Me, args.Viewnum
	switch server {
	case vs.view.Primary:
		if viewNum == vs.view.Viewnum {
			vs.viewAcked = true
			vs.timeStamp[server] = time.Now()
		} else {
			// primary has crashed
			if vs.viewAcked && vs.view.Backup != "" {
				vs.updateView(vs.view.Backup, "")
			}
		}

	case vs.view.Backup:
		if viewNum == vs.view.Viewnum {
			vs.timeStamp[server] = time.Now()
		} else {
			// backup has crashed
			vs.updateView(vs.view.Primary, "")
		}

	default:
		vs.addServer(server)

	}

	reply.View = *vs.view //right?
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	reply.View = *vs.view
	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//

func (vs *ViewServer) tick() {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	for server, stamp := range vs.timeStamp {
		if time.Since(stamp) >= DeadPings*PingInterval {
			vs.removeServer(server)
		}
	}

}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me

	// Your vs.* initializations here.
	vs.mu = sync.Mutex{}
	vs.timeStamp = make(map[string]time.Time)
	vs.view = &View{0, "", ""}
	vs.viewAcked = true

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
