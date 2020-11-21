package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

var (
	node             Node
	mu               sync.Mutex
	VoteTimer        = time.NewTicker(100 * time.Second)
	Conns            []*rpc.Client
	LinkType         []LinkStateType
)
var config = getConfig("./config.json")

type RaftRpc struct{
}
type Config struct{
	Ip string
	Peers []string
	Id int64
	ClusterSize int64
}


func (r *RaftRpc) HeartBeat(request *Raft,response *Raft) error {
	switch request.Type{
	case MessageType_MsgBeat:
		_ = Tick(request,response)
	case MessageType_MsgVote:
		_ = Vote(request,response)
	}
	return nil
}
func Vote(request *Raft,response *Raft) error {
	if request.Term < node.Term{
		response.Term = node.Term
		response.Log = "vote rejected"
		return nil
	}
	if request.Term > node.Term{
		mu.Lock()
		node.Term = request.Term
		node.VoteFor = request.VotedFor
		mu.Unlock()
		BecomeFollower()
		response.Log = "vote success"
		return nil
	}
	if node.CurrentStateType == StateType_Follower{
		if node.VoteFor == 0{
			mu.Lock()
			node.VoteFor = request.VotedFor
			node.Term = request.Term
			mu.Unlock()
			ResetVoteTimer(VoteTimer)
			response.Log = "vote success"
		}else{
			response.Log = "vote rejected"
		}
		return nil
	}
	response.Log = "vote rejected"
	return nil
}
func Tick(request *Raft,response *Raft) error {
	if node.Term > request.Term{
		response.Term = node.Term
		response.Log = "become follower"
		return nil
	}
	BecomeFollower()
	mu.Lock()
	node.Term = request.Term
	mu.Unlock()
	return nil
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
func Connect(index int){
	LinkType[index] = LinkStateType_Connecting
	var err error
LOOP:
	Conns[index],err = rpc.DialHTTP("tcp", config.Peers[index])
	if err != nil{
		goto LOOP
	}
	LinkType[index] = LinkStateType_Connected
	Log("connect peer ",config.Peers[index]," successful")
}
func Init(){
	for i := 0;i < int(config.ClusterSize);i ++{
		LinkType = append(LinkType, LinkStateType_Disconnected)
		Conns = append(Conns, nil)
	}
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
		}
	}(config.Ip)
	BecomeFollower()
	fmt.Println("become follower")
}

func BecomeLeader(){
	mu.Lock()
	node.CurrentStateType = StateType_Leader
	Log("I am leader in term ",node.Term," when ",time.Now())
	mu.Unlock()
	for i := 0;i < int(config.ClusterSize);i ++{
		go startHeartbeat(i)
	}
}
func BecomeFollower(){
	mu.Lock()
	Log("I am follower in term ",node.Term," when ",time.Now())
	node.CurrentStateType = StateType_Follower
	ResetVoteTimer(VoteTimer)
	mu.Unlock()
}
func BecomeCandidate(){
	for i := 0;i < int(config.ClusterSize);i ++{
		if LinkType[i] != LinkStateType_Disconnected{
			continue
		}
		LinkType[i] = LinkStateType_Disconnected
		if config.Id == int64(i + 1){
			continue
		}
		go Connect(i)
	}
	mu.Lock()
	Log("I am candidate in term ",node.Term," when ",time.Now())
	node.CurrentStateType = StateType_Candidate
	node.Term ++
	node.VoteCount = 1
	ResetVoteTimer(VoteTimer)
	mu.Unlock()
}
func ResetVoteTimer(ticker *time.Ticker){
	node.ElectionTimeout = int64(rand.Int() % 150 + 150)
	ticker.Reset(time.Duration(node.ElectionTimeout) * time.Millisecond)
}
func ResetHeartBeatTimer(ticker *time.Ticker){
	node.HeartBeatTimeout = int64(rand.Int() % 30 + 15)
	ticker.Reset(time.Duration(node.HeartBeatTimeout) * time.Millisecond)
}

func Election(){
	for{
		select{
			case <- VoteTimer.C:
				Log("Election Time Out")
				BecomeCandidate()
				raft := Raft{
					Type: MessageType_MsgVote,
					Term: node.Term,
					VotedFor: config.Id,
					Log: "",
				}
				for i := 0;i < int(config.ClusterSize);i ++{
					if config.Id == int64(i + 1) || Conns[i] == nil{
						continue
					}
					go func(index int,rf Raft){
						err := Conns[index].Call("RaftRpc.HeartBeat",rf,&rf)
						if err != nil && LinkType[index] == LinkStateType_Connected{
							Log("TRY")
							go Connect(index)
						}
						if err != nil{
							return
						}
						if rf.Log == "vote success" && node.CurrentStateType == StateType_Candidate{
							mu.Lock()
							node.VoteCount ++
							mu.Unlock()
						}
						if rf.Log == "vote rejected"{
							mu.Lock()
							node.Term = raft.Term
							mu.Unlock()
						}
						if node.VoteCount > config.ClusterSize / 2 && node.CurrentStateType == StateType_Candidate{
							BecomeLeader()
							VoteTimer.Stop()
						}
					}(i,raft)
				}
		}
	}
}
func startHeartbeat(index int){
	raft := Raft{
		Term: node.Term,
		Log: "",
	}
	HeartBeatTimer := time.NewTicker(100 * time.Second)
	ResetHeartBeatTimer(HeartBeatTimer)
	for{
		if node.CurrentStateType != StateType_Leader{
			return
		}
		select {
			case <-HeartBeatTimer.C:
				Log("heart beat for peer ",config.Peers[index])
				if Conns[index] == nil{
					continue
				}
				err := Conns[index].Call("RaftRpc.HeartBeat",raft,&raft)
				if err != nil && LinkType[index] == LinkStateType_Connected{
					Connect(index)
				}
				if raft.Log == "become follower"{
					BecomeFollower()
					HeartBeatTimer.Stop()
					return
				}
		}
	}
}
func main(){
	done := make(chan struct{})
	Init()
	go Election()
	<- done
}