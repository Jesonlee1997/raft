package raft

import (
	"time"
	"util/log"
	_ "util"
	"util"
)

type LogEntry struct {
	Term int
	Cmd  interface{}
}

type ApplyEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int //index of log entry immediately preceding new ones
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}

func (args *ApplyEntriesArgs) String() string {
	return util.String(args)
}

type ApplyEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) doApplyEntries() {
	for peer := range rf.peers {
		p := peer
		go func() {
			reply := &ApplyEntriesReply{}
			args := &ApplyEntriesArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.LeaderCommit = rf.commitIndex
			args.PrevLogIndex = rf.nextIndexes[p] - 1
			args.PrevLogTerm = rf.getLogTerm(args.PrevLogIndex)
			args.Entries = rf.logs[rf.nextIndexes[p]:]
			ok := rf.sendApplyEntries(p, args, reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if len(args.Entries) == 0 {
				log.Debug("HeartBeat:peer", p, "receive", args, "reply", reply)
			} else {
				log.Debug("ApplyEntries RPC:peer", p, "receive", args, "reply", reply)
			}
			//在请求过程中发生了状态变迁
			if rf.state != StateLeader || rf.currentTerm != args.Term {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.convertToFollower(reply.Term)
				return
			}

			// args.PreLogTerm == 0时是一定满足一致性检查的
			if reply.Success {
				if len(args.Entries) > 0 {
					rf.nextIndexes[p] = util.Max(rf.nextIndexes[p], args.PrevLogIndex + len(args.Entries) + 1)
					rf.matchIndexes[p] = rf.nextIndexes[p] - 1
					rf.updateCommitIndex()
				} else {
					rf.matchIndexes[p] = rf.nextIndexes[p] - 1
				}
			} else {
				rf.nextIndexes[p] = rf.getPreTermFirstIndex(args.PrevLogTerm, args.PrevLogIndex) + 1
			}
		}()
	}
	time.Sleep(100 * time.Millisecond)
}

// ApplyEntries
//1. Reply false if term < currentTerm (§5.1)
//2. Reply false if log doesn’t contain an entry at prevLogIndex
//whose term matches prevLogTerm (§5.3)
//3. If an existing entry conflicts with a new one (same index
//but different terms), delete the existing entry and all that
//follow it (§5.3)
//4. Append any new entries not already in the log
//5. If leaderCommit > commitIndex, set commitIndex =
//min(leaderCommit, index of last new entry)
func (rf *Raft) ApplyEntries(args *ApplyEntriesArgs, reply *ApplyEntriesReply) {
	rf.mu.Lock()
	defer rf.receiveRPC(0)
	defer rf.mu.Unlock()
	defer func() {
		if reply.Success && len(args.Entries) != 0 {
			rf.persist()
		}
	}()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		if rf.state == StateCandidate {
			rf.endElect()
		}
		rf.convertToFollower(args.Term)
	}

	if rf.state == StateCandidate {
		rf.endElect()
		rf.convertToFollower(args.Term)
	}

	//log match
	if args.PrevLogIndex > rf.getLastLogIndex() || args.PrevLogTerm != rf.getLogTerm(args.PrevLogIndex) {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if len(args.Entries) != 0 {
		rf.logs = append(rf.logs[0:args.PrevLogIndex+1], args.Entries...)
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = util.Min(args.LeaderCommit, rf.getLastLogIndex())
	}

	if rf.commitIndex > rf.lastApplied {
		go func(lastApplied int, commitIndex int) {
			i := lastApplied + 1
			for ; i < commitIndex+1; i++ {
				entry := rf.logs[i]
				applyMsg := &ApplyMsg{Index: i, Command: entry.Cmd}
				log.Debug("ApplyMsg:", applyMsg, "peer",rf.me)
				rf.applyCh <- *applyMsg
				rf.lastApplied++
			}
		}(rf.lastApplied, rf.commitIndex)
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	return
}
