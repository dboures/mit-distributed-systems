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

import (
	//	"bytes"
	"math"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"

	"fmt"
	"math/rand"
	"time"

	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Log struct {
	Term int
	// Command
}

type PeerStatus int

const (
	Follower  PeerStatus = 0
	Candidate PeerStatus = 1
	Leader    PeerStatus = 2
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// latest term server has seen
	currentTerm int
	votedFor    int
	logs        []Log
	// index of highest log entry known to be committed
	commitIndex int
	//index of highest log entry applied to state machine
	lastApplied int

	role          PeerStatus
	lastHeartbeat time.Time
	applyCh       chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	term := rf.currentTerm
	isleader := rf.role == Leader
	rf.mu.Unlock()

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	rf.lastHeartbeat = time.Now()

	if args.Term > rf.currentTerm {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		rf.role = Follower

		// fmt.Printf("Peer %d votedFor: %d. Reqor: %d, %d, %d, %d, %d\n", rf.me, rf.votedFor, args.CandidateId, args.LastLogIndex, rf.commitIndex, args.LastLogTerm, rf.currentTerm)
		// fmt.Printf("%d, >= %d, %t\n", args.LastLogIndex, rf.commitIndex, args.LastLogIndex >= rf.commitIndex)
		// fmt.Printf("%d, >= %d, %t\n", args.LastLogTerm, rf.currentTerm, args.LastLogTerm >= rf.currentTerm)

	} else if args.Term == rf.currentTerm {
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && args.LastLogIndex >= rf.commitIndex && args.LastLogTerm >= rf.currentTerm {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.role = Follower
		}
	} else {
		reply.VoteGranted = false
	}

	fmt.Printf("%s \t Peer %d sending leader vote: %t to %d. VotedFor: %d Peer term: %d, Arg term: %d\n", time.Now().Truncate(time.Millisecond), rf.me, reply.VoteGranted, args.CandidateId, rf.votedFor, rf.currentTerm, args.Term)
	rf.mu.Unlock()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []interface{}
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	// fmt.Printf("Peer %d received heartbeat \n", rf.me)
	rf.lastHeartbeat = time.Now()

	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.role = Follower
	} else if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	// else if args.Term == rf.currentTerm {

	// } else {
	// 	reply.Success = false
	// }

	// fmt.Printf("Peer %d received heartbeat and now its a %d\n", rf.me, rf.role)
	// if args.Term >= rf.currentTerm && rf.role == Follower {
	// 	rf.commitIndex += 1
	// 	reply.Success = true
	// } else {
	// 	reply.Success = false
	// }
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	index := rf.commitIndex + 1
	term := rf.currentTerm
	isLeader := rf.role == Leader
	fmt.Printf("start called on peer %d  \n", rf.me)
	rf.mu.Unlock()
	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// sleep
		interval := 500 + (rand.Float64() * 800)
		duration := time.Duration(math.Round(interval)) * time.Millisecond

		time.Sleep(duration)

		rf.mu.Lock()
		nextElection := rf.lastHeartbeat.Add(time.Second) // 1 sec after last heartbeat
		now := time.Now()

		// fmt.Printf("Peer %d last heartbeat at: %s \n", rf.me, rf.lastHeartbeat.Truncate(time.Second))
		// fmt.Printf("Peer %d current time: %s, next election at: %s \n", rf.me, time.Now().Truncate(time.Second), nextElection.Truncate(time.Second))
		isLeader := rf.role == Leader
		rf.mu.Unlock()

		if now.After(nextElection) && !isLeader {
			doLeaderElection(rf)
		}
	}
}

func (rf *Raft) heartbeat() {
	for rf.killed() == false {

		duration := time.Duration(110 * time.Millisecond)
		time.Sleep(duration)

		rf.mu.Lock()
		if rf.role == Leader {
			// fmt.Printf("Leader %d sending heartbeat\n", rf.me)
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: 0, //TODO
				PrevLogTerm:  rf.currentTerm,
				Entries:      make([]interface{}, 0), // TODO
			}

			rf.mu.Unlock()

			for i := range rf.peers {
				if i != rf.me {
					reply := AppendEntriesReply{}
					go func(k int) {
						rf.sendAppendEntries(k, &args, &reply)
					}(i)
				}
			}
		} else {
			rf.mu.Unlock()
		}
	}
}

func doLeaderElection(rf *Raft) {
	if rf.killed() == false {
		rf.mu.Lock()
		rf.votedFor = rf.me
		rf.role = Candidate
		rf.currentTerm = rf.currentTerm + 1
		fmt.Printf("%s \t Peer %d starting leader election at term: %d\n", time.Now().Truncate(time.Millisecond), rf.me, rf.currentTerm)
		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: rf.commitIndex,
			LastLogTerm:  rf.currentTerm,
		}

		rf.mu.Unlock()

		var yesTally uint64
		for i := range rf.peers {
			reply := RequestVoteReply{}

			go func(k int, args RequestVoteArgs, reply RequestVoteReply) {
				answer := rf.sendRequestVote(k, &args, &reply)
				if answer {
					handleLeaderVote(rf, args, reply, &yesTally)
				}
			}(i, args, reply)
		}
	}
}

func handleLeaderVote(rf *Raft, args RequestVoteArgs, reply RequestVoteReply, yesTally *uint64) {
	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.role = Follower
		rf.votedFor = -1
	} else if reply.VoteGranted {
		atomic.AddUint64(yesTally, 1)
	}

	if *yesTally > uint64(len(rf.peers)/2) && rf.role != Leader {
		rf.role = Leader
		fmt.Printf("%s \t Peer %d is now the leader\n", time.Now().Truncate(time.Millisecond), rf.me)
		blankArgs := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: 0, //TODO
			PrevLogTerm:  rf.currentTerm,
			Entries:      make([]interface{}, 0),
		}

		for i := range rf.peers {
			if i != rf.me {
				reply := AppendEntriesReply{}
				go func(k int) {
					rf.sendAppendEntries(k, &blankArgs, &reply)
				}(i)
			}
		}
	}
	rf.mu.Unlock()
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.role = Follower
	rf.logs = make([]Log, 0)
	rf.logs = append(rf.logs, Log{Term: 0})
	rf.lastHeartbeat = time.Now()
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.votedFor = -1
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.heartbeat()

	return rf
}
