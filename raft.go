package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)
type StateType int
const (
	Follower StateType = 1
	Leader StateType = 2
	Candidate StateType = 3
)
var (
	mu sync.Mutex
	voteTimer = time.NewTicker(100 * time.Second)
	heartbeatTimer *time.Ticker
	voteTimeout int
	heartbeatTimeout int
	currentStateType StateType = Follower
	conns [3]*rpc.Client
)

type RaftRpc struct{

}
type Config struct{
	Ip string
	Port string
	Peers []string
	ClusterSize int64
	VoteCount int64
	Term int64
	Id int64
	Leader int64
}
var config = getConfig("./config.json")
func (r *RaftRpc) HeartBeat(request Raft,response *Raft) error {
	switch request.Type{
	case MessageType_MsgBeat:
		err := Tick(request,response)
		if err != nil{
			Log(err.Error())
		}
		break
	case MessageType_MsgVote:
		err := Vote(request,response)
		if err != nil{
			Log("vote failed")
		}
		break
	}

	return nil
}
func Vote(request Raft,response *Raft) error {
	if currentStateType == Candidate{
		return errors.New("candidate can not vote for me")
	}
	if currentStateType == Leader{
		return errors.New("leader can not vote for me")
	}
	if request.CurrentTerm < config.Term{
		return errors.New("term is smaller than follower")
	}
	config.Leader = request.VotedFor
	config.Term = response.CurrentTerm
	response.Alive = true
	response.Reject = true
	voteTimeout = rand.Int() % 3 + 3
	voteTimer.Reset(time.Duration(voteTimeout) * time.Second)
	return nil
}
func Tick(request Raft,response *Raft) error {
	voteTimeout = rand.Int() % 3 + 3
	voteTimer.Reset(time.Duration(voteTimeout) * time.Second)
	if request.CurrentTerm < config.Term{
		response.Alive = true
		response.Reject = true
		response.CurrentTerm = config.Term
		return errors.New("heart beat rejected")
		//need polish
	} else {
		response.Alive = true
		response.Reject = false
		config.Leader = request.VotedFor
		config.Term = request.CurrentTerm
		Log("heart beat from ",request.VotedFor," in term ",config.Term)
		return nil
	}
}
func Log(val... interface{}){
	fmt.Println(val)
}
func getConfig(path string) Config {
	val := Config{}
	file,err := ioutil.ReadFile(path)
	if err != nil{
		log.Fatal(err)
		return Config{}
	}
	err = json.Unmarshal(file,&val)
	if err != nil{
		log.Fatal(err)
		return Config{}
	}
	return val
}

func Init(){
	err := rpc.Register(new(RaftRpc))
	if err != nil{
		Log("rpc register error: ",err)
		return
	}
	rpc.HandleHTTP()
	go func(addr string){
		err = http.ListenAndServe(addr,nil)
		if err != nil{
			log.Fatal(err)
			return
		}
	}(config.Ip + ":" + config.Port)
	for i := 0;i < int(config.ClusterSize - 1);i ++{
		go func(j int){
			var err error
		LOOP:
			conns[j],err = rpc.DialHTTP("tcp", config.Peers[j])
			if err != nil{
				goto LOOP
			}
		}(i)
	}
	go election()
}
func election(){
	currentStateType = Candidate
	voteTimeout = rand.Int() % 3 + 3
	voteTimer.Reset(time.Duration(voteTimeout) * time.Second)
	for{
		select{
			case <- voteTimer.C:
				Log("vote timeout")
				currentStateType = Candidate
				config.VoteCount = 1
				config.Term += 1
				voteTimeout = rand.Int() % 3 + 3
				voteTimer.Reset(time.Duration(voteTimeout) * time.Second)
				raft := Raft{
					Type: MessageType_MsgVote,
					CurrentTerm: config.Term,
					VotedFor: config.Id,
				}
				for i := 0;i < int(config.ClusterSize - 1);i ++{
					go func(index int){
						if conns[index] == nil{
							return
						}
						err := conns[index].Call("RaftRpc.HeartBeat",raft,&raft)
						if err != nil{
							Log(err)
						}else{
							mu.Lock()
							config.VoteCount ++
							mu.Unlock()
							Log("got voted in term: ",config.Term)
							Log("vote count ",config.VoteCount)
							if config.VoteCount > config.ClusterSize / 2{
								voteTimer.Stop()
								if currentStateType == Leader{
									return
								}
								mu.Lock()
								currentStateType = Leader
								mu.Unlock()
								Log("i am leader")
								becomeLeader()
							}

						}
					}(i)
				}
		}
	}
}
func becomeLeader(){
	for i := 0;i < int(config.ClusterSize - 1);i ++{
		go startHeartbeat(i)
	}
}
func startHeartbeat(index int){
	raft := Raft{
		Alive: false,
		VotedFor: 0,
		CurrentTerm: config.Term,
	}
	heartbeatTimeout = rand.Int() % 100 + 100
	heartbeatTimer = time.NewTicker((time.Duration(heartbeatTimeout)) * time.Millisecond)
	raft.Alive = false
	raft.VotedFor = config.Id
	for{
		select {
			case <-heartbeatTimer.C:
				if conns[index] == nil{
					continue
				}
				err := conns[index].Call("RaftRpc.HeartBeat",raft,&raft)
				if err != nil{
					Log(err)
				}
				if raft.Alive == false{
					Log(config.Peers[index]," is dead.")
					go func(i int){
						var err error
					LOOP:
						conns[i],err = rpc.DialHTTP("tcp", config.Peers[i])
						if err != nil{
							goto LOOP
						}
					}(index)
				}else{
					Log(config.Peers[index]," is alive.")
					raft.Alive = false
				}
		}
	}
}
func main(){
	done := make(chan struct{})
	go Init()
	<- done
}