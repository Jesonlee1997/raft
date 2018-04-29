package raft

import (
	"fmt"
	"strconv"
	"util/log"
	"util"
)

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int //candidate’s term
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	fmt.Stringer
}

func (args *RequestVoteArgs) String() string {
	result := "{Term:" + strconv.Itoa(args.Term) +
		",CandidateId:" + strconv.Itoa(args.CandidateId) +
		",LastLogIndex:" + strconv.Itoa(args.LastLogIndex) +
		",LastLogTerm:" + strconv.Itoa(args.LastLogTerm) + "}"
	return result
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
// 1. Reply false if term < currentTerm
// 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log,
// 	  grant vote(candidate的log至少要和receiver的log一样新)
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer log.Info("RequestVote RPC:peer", strconv.Itoa(rf.me), "receive", args, " reply", util.String(reply))
	defer rf.receiveRPC(1)
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		// 对于term更高的请求，无条件转变成Follower
		// 对于Candidate状态，需结束其选举过程
		rf.state = StateFollower
		if rf.state == StateCandidate {
			rf.endElect()
		}
		rf.changeTerm(args.Term, -1)
	}

	//保证对于相同term的只投一次票，所以一个Term最多只有一个leader
	if rf.state == StateFollower &&
		rf.votedFor == -1 {
		if args.LastLogTerm > rf.getLastLogTerm() ||
			(args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex()) {
			rf.votedFor = args.CandidateId
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			rf.persist()
			return
		}
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
}
