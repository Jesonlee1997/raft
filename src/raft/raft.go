package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import (
	"bytes"
	"encoding/gob"

	"sort"
	"sync/atomic"
	"fmt"
	"util/log"
	"time"
	"util"
	"math/rand"
	"rpc"
)

// Because the tester limits you to 10 heartbeats per second,
// you will have to use an election timeout larger than the paper's 150 to 300 milliseconds,
// but not too large, because then you may fail to elect a leader within five seconds.
const MinElectTimeout = 700
const MaxElectTimeout = 900
const retryTimes = 3

type State int32

const (
	StateFollower State = iota
	StateLeader
	StateCandidate
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//

type Raft struct {
	mu        sync.Mutex
	peers     []*rpc.Client
	persister *Persister
	me        int // index into peers[]
	applyCh   chan ApplyMsg

	termElectState map[int]chan interface{}
	rpcChan        chan interface{}
	state          State

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// -------------------Persistent state on all servers-----------------
	currentTerm int //latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int //candidateId that received vote in current term (or null if none)

	//log entries; each entry contains command for state machine,
	// and term when entry was received by leader (first index is 1)
	logs []*LogEntry //第0索引不存储日志，len(logs)=maxLogIndex+1
	// -------------------Persistent state on all servers-----------------

	//-----------------Volatile state on all servers-----------------------
	commitIndex int //index of highest log entry known to be committed, initialize to 0
	lastApplied int //index of highest log entry applied to state machine, initialize to 0
	//-----------------Volatile state on all servers-----------------------

	//--------------volatile on leader. Reinitialized after election--------------
	// for each server, index of the next log entry to send
	// to that server (initialized to leader last log index + 1)
	nextIndexes []int

	//for each server, index of highest log entry known to be replicated on server
	// (initialized to 0, increases monotonically)
	matchIndexes []int
	//--------------volatile on leader-------------------------------------

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here.
	return rf.currentTerm, rf.state == StateLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logs)
}

// 0代表是ApplyEntries
// 1代表是requestVote
func (rf *Raft) receiveRPC(v int) {
	if rf.state == StateFollower {
		rf.rpcChan <- v
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendApplyEntries(server int, args *ApplyEntriesArgs, reply *ApplyEntriesReply) bool {

	ok := rf.peers[server].Call("Raft.ApplyEntries", args, reply)
	return ok
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.logs) - 1
}

func (rf *Raft) getLastLogTerm() int {
	return rf.logs[rf.getLastLogIndex()].Term
}

func (rf *Raft) getLogTerm(index int) int {
	return rf.logs[index].Term
}

//index是这个term的其中一条log的索引
func (rf *Raft) getPreTermFirstIndex(term int, termIndex int) int {
	if term == 1 {
		return 0
	}

	const findTerm = 0
	const findIndex = 1
	state := findTerm
	var preTerm = -1
	for i := termIndex; i >= 0; i-- {
		if state == findTerm {
			if rf.logs[i].Term < term {
				state = findIndex
				preTerm = rf.logs[i].Term
			}
		} else {
			if rf.logs[i].Term != preTerm {
				return i + 1
			}
		}
	}
	panic(term)
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (idx int, currentTerm int, isLeader bool) {
	if rf.state != StateLeader {
		return -1, rf.currentTerm, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	logEntry := &LogEntry{rf.currentTerm, command}
	rf.logs = append(rf.logs, logEntry)
	log.Info("start", logEntry, "leader", rf.me, "index", rf.getLastLogIndex())
	return rf.getLastLogIndex(), rf.currentTerm, true
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

var rd = rand.New(rand.NewSource(time.Now().Unix()))
// 单位：毫秒
func getElectionTimeout() time.Duration {
	timeout := MinElectTimeout + rd.Int63n(MaxElectTimeout-MinElectTimeout)
	return util.GetMillDuration(timeout)
}

func (rf *Raft) doElect() {
	var electState = make(chan interface{}, 1)
	var electClosed = make(chan interface{}, len(rf.peers)) //选举成功即被closed，转变为follower也被closed
	for range rf.peers {
		electClosed <- 1
	}

	var voteCount int32 = 0
	var voteCountPtr = &voteCount

	log.Info(rf.me, " start elected...", "term", rf.currentTerm+1)
	rf.mu.Lock()
	if rf.state != StateCandidate {
		rf.mu.Unlock()
		return
	}
	rf.changeTerm(rf.currentTerm+1, rf.me)
	requestVoteArgs := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm()}
	rf.termElectState[rf.currentTerm] = electState

	*voteCountPtr++
	rf.mu.Unlock()

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		p := peer
		go func() {

			voteReply := &RequestVoteReply{}
			ok := rf.sendRequestVote(p, requestVoteArgs, voteReply)

			if !ok {
				return
			}

			electState, exists := rf.termElectState[requestVoteArgs.Term]
			//说明选举已经结束

			if !exists {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			_, open := <-electClosed
			// 说明在选举过程中转换为了Follower直接返回
			if !open {
				return
			}

			//如果在投票期间转变成了Follower
			if rf.state == StateFollower {
				//结束该轮选举
				util.CloseChan(electClosed)
				electState <- false
				return
			}

			if voteReply.Term == requestVoteArgs.Term {
				if voteReply.VoteGranted {
					log.Info("peer", rf.me, "vote granted from peer", p, "voteCount=", voteCount+1)
					if atomic.AddInt32(voteCountPtr, 1) >= int32(rf.getMajority()) && rf.state == StateCandidate {
						rf.convertToLeader()
						util.CloseChan(electClosed)
						electState <- true
					}
				} else {
					log.Info(rf.me, "vote rejected from", p, "voteCount=", voteCount)
				}
			} else if voteReply.Term > requestVoteArgs.Term {
				log.Info("vote req's term < reply's term", "peer", rf.me, "send request vote to", p)
				rf.convertToFollower(voteReply.Term)
				electState <- false
			}
		}()
	}

	timeout := getElectionTimeout()
	_, isTimeout := util.Timeout(electState, timeout)
	if isTimeout {
		log.Info("ElectTimeout:", timeout, "ms", "peer", rf.me)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	delete(rf.termElectState, requestVoteArgs.Term)
	_, open := <-electClosed
	if open {
		util.CloseChan(electClosed)
	}
}

// 外层需加锁
func (rf *Raft) convertToFollower(term int) {
	switch rf.state {
	case StateLeader:
		log.Info("StateChange:", "leader", rf.me, "convert to follower")
	case StateCandidate:
		log.Info("StateChange:", "candidate", rf.me, "convert to follower")
	case StateFollower:
		log.Info("StateChange:", "follower", rf.me, "convert to follower")
	}
	rf.state = StateFollower
	rf.changeTerm(term, -1)
}

func (rf *Raft) convertToLeader() {
	for i := range rf.nextIndexes {
		rf.nextIndexes[i] = rf.getLastLogIndex() + 1
		rf.matchIndexes[i] = 0
	}
	rf.state = StateLeader
	log.Info("StateChange:", "peer", rf.me, "become to leader")
}

func (rf *Raft) endElect() {
	electState, exists := rf.termElectState[rf.currentTerm]
	if exists {
		electState <- false
	}
}

//找出majority的server都大于的一个整数
func (rf *Raft) updateCommitIndex() {
	var indexes = make([]int, len(rf.matchIndexes), len(rf.matchIndexes))
	for i := range indexes {
		indexes[i] = rf.matchIndexes[i]
	}

	sort.Ints(indexes)
	oldIndex := rf.commitIndex
	rf.commitIndex = indexes[rf.getMajority()-1]
	if oldIndex != rf.commitIndex {
		log.Info("CommitIndex changed", oldIndex, "to", rf.commitIndex)
	}
}

func (rf *Raft) getMajority() int {
	return len(rf.peers)/2 + 1
}

func (rf *Raft) PrintState() {
	var str string
	switch rf.state {
	case StateLeader:
		str = "Leader"
	case StateFollower:
		str = "Follower"
	case StateCandidate:
		str = "Candidate"
	}
	fmt.Println("State:", "peer", rf.me, "is", str)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*rpc.Client, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me

	rf.votedFor = -1
	rf.termElectState = make(map[int]chan interface{})
	rf.rpcChan = make(chan interface{}, len(peers))

	rf.logs = append(rf.logs, &LogEntry{0, -1})
	rf.nextIndexes = make([]int, len(rf.peers))
	rf.matchIndexes = make([]int, len(rf.peers))

	rf.persister = persister

	rf.applyCh = applyCh



	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Your initialization code here.
	// create a background goroutine that will kick off leader election
	// periodically by sending out RequestVote RPCs when it hasn't heard from another peer for a while.
	go rf.StartPeer()

	return rf
}

func (rf *Raft) StartPeer() {
	for true {
		if rf.state == StateFollower {
			timeout := getElectionTimeout()
			_, isTimeout := util.Timeout(rf.rpcChan, timeout)
			if isTimeout {
				// election timeout,转变状态，开始一轮选举
				log.Info("StateChange:", "peer", rf.me, " rpc timeout:", timeout, "turn to Candidate")
				rf.state = StateCandidate
			} else {
				//log.Debug("peer ", rf.me, "receive rpc", val)
			}
		} else if rf.state == StateCandidate {
			// 开始一轮选举，如果一段时间内没有winner，开始下一轮
			rf.doElect()
		} else { // leader
			//10heartbeat per sec
			rf.doApplyEntries()
		}
	}
}
func (rf *Raft) changeTerm(term int, voteFor int) {
	rf.persist()
	rf.currentTerm = term
	rf.votedFor = voteFor
}
