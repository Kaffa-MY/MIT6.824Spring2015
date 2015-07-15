package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
	OpGet          = "Get"
	OpPut          = "Put"
	OpAppend       = "Append"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.

	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string //"Put" or "Append"
	ClientId  int64
	Seq       int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.
type ForwardArg struct {
	Operation string
	Key       string
	Value     string
	ClientId  int64
	Seq       int64
}

type FordwardReply struct {
	Err   Err
	Value string
}

type SyncArgs struct {
	KV  map[string]string
	Seq map[int64]int64
}

type SyncReply struct {
	Err Err
}
