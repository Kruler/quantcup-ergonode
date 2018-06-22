package main

import (
	"fmt"
	"github.com/halturin/ergonode"
	"github.com/halturin/ergonode/etf"
	"github.com/thejerf/suture"
)

type goGenServ struct {
	servername string
	ergonode.GenServer
	completeChan chan bool
	e            Engine
	iscomplete   bool
}

const (
	batchSize   int = 10
	replayCount     = 200
)

var n *ergonode.Node
var sup *suture.Supervisor

func main() {
	fmt.Println("start")
	n = ergonode.Create("quantcup@localhost", 7000, "abc")

	// Create channel to receive message when main process should be stopped

	// Initialize new instance of goGenServ structure which implements Process behaviour
	gs := new(goGenServ)
	gs.servername = "si1806"
	// gs2 := new(goGenServ)
	// gs2.servername = "si1805"
	sup = suture.New("engine_supervisor", suture.Spec{})
	sup.Add(gs)
	// sup.Add(gs2)
	sup.Serve()

	// Spawn process with one arguments

	// Wait to stop

	return
}

func (gs *goGenServ) String() string {
	return gs.servername
}

func (gs *goGenServ) Serve() {
	fmt.Println("server start")
	completeChan := make(chan bool)
	n.Spawn(gs, completeChan)
	<-completeChan
	fmt.Println("debug")
}

func (gs *goGenServ) Stop() {
	gs.completeChan <- true
}

func (gs *goGenServ) Complete() bool {
	return gs.iscomplete
}

func (gs *goGenServ) Init(args ...interface{}) interface{} {
	// Self-registration with name go_srv
	gs.e.Reset()
	gs.Node.Register(etf.Atom(gs.servername), gs.Self)
	fmt.Println("loop exist", gs.servername)
	// Store first argument as channel
	gs.completeChan = args[0].(chan bool)

	return nil
}

// HandleCast serves incoming messages sending via gen_server:cast
func (gs *goGenServ) HandleCast(message *etf.Term, state interface{}) (code int, stateout interface{}) {
	fmt.Println("HandleCast: %#v", *message)
	stateout = state
	code = 0
	// Check type of message
	switch req := (*message).(type) {
	case etf.Tuple:
		if len(req) == 3 {
			switch act := req[0].(type) {
			case etf.Atom:
				if string(act) == "submit" {
					// var self_pid etf.Pid = gs.Self
					var order Order
					raworder := req[1].(etf.Tuple)
					order.price = Price(raworder[1].(int))
					order.symbol = raworder[0].(string)
					order.side = Side(raworder[2].(int))
					order.size = Size(raworder[3].(int))
					order.trader = raworder[4].(string)
					if order.size == 0 {
						return
					}
					if order.symbol == gs.servername {
						gs.e.node = gs.Node
						gs.e.topid = req[2].(etf.Pid)
						gs.e.Limit(order)
						rep := etf.Term(etf.Tuple{etf.Atom("submit sucess")})
						gs.Send(req[2].(etf.Pid), &rep)
					} else {
						rep := etf.Term(etf.Tuple{etf.Atom("invalid symbol"), gs.Self})
						gs.Send(req[2].(etf.Pid), &rep)
					}

				}
			}
		}
	case etf.Atom:
		// If message is atom 'stop', we should say it to main process
		if string(req) == "stop" {
			gs.iscomplete = true
			gs.completeChan <- true
		}
		if string(req) == "stop1" {
			gs.completeChan <- true
		}
	}
	return
}

// HandleCall serves incoming messages sending via gen_server:call
func (gs *goGenServ) HandleCall(from *etf.Tuple, message *etf.Term, state interface{}) (code int, reply *etf.Term, stateout interface{}) {
	// fmt.Printf("HandleCall: %#v, From: %#v\n", *message, *from)

	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Call recovered: %#v\n", r)
		}
	}()

	stateout = state
	code = 1
	replyTerm := etf.Term(etf.Tuple{etf.Atom("error"), etf.Atom("unknown_request")})
	reply = &replyTerm
	return
}

// HandleInfo serves all another incoming messages (Pid ! message)
func (gs *goGenServ) HandleInfo(message *etf.Term, state interface{}) (code int, stateout interface{}) {
	fmt.Println("HandleInfo: %#v\n", *message)
	stateout = state
	code = 0
	return
}

// Terminate called when process died
func (gs *goGenServ) Terminate(reason int, state interface{}) {
	fmt.Println("Terminate: %#v\n", reason)
}
