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
	"log"
	"math/rand"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

func init() {
	rand.Seed(time.Now().Unix())
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

type Role int

// timeout
const (
	ELECTIONTIMEOUT  = 300
	HEARTBEATTIMEOUT = 150
)

// 3 state
const (
	LEADER Role = iota
	CANDIDATE
	FOLLOWER
)

type LogEntry struct {
	Term    int
	Idx     int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 2A
	role                Role
	currentTerm         int
	votedFor            int
	electionTimer       *time.Timer
	appendEntriesTimers []*time.Timer
	// 2A 暂时未用到
	logEntries  []LogEntry
	commitIndex int
	lastApplied int

	// 成为leader时才(重新)需要初始化
	nextIndex  []int
	matchIndex []int

	applyCh chan ApplyMsg
	killCh  chan struct{}

	debugMode bool
}

func (rf *Raft) Log(msg ...interface{}) {
	if rf.debugMode {
		logs := append([]interface{}{rf.me, "in term: ", rf.currentTerm}, msg...)
		log.Println(logs...)
	}
}

func (rf *Raft) setElectionTime() {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(time.Duration(ELECTIONTIMEOUT + rand.Int63n(ELECTIONTIMEOUT)))
}

func (rf *Raft) setHeartBeatTime(peerIdx int) {
	rf.appendEntriesTimers[peerIdx].Stop()
	rf.appendEntriesTimers[peerIdx].Reset(HEARTBEATTIMEOUT)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.role == LEADER

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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

//
// restore previously persisted state.
//
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
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

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	// 还有暂未用到
	PreLogIndex int
	PreLogTerm  int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.Log("receive heartbeats")
	reply.Term = rf.currentTerm

	if rf.currentTerm > args.Term {
		return
	}

	rf.currentTerm = args.Term
	rf.role = FOLLOWER
	// 接收到心跳，重置选举超时计时器
	rf.setElectionTime()
	// 目前不涉及到索引
	reply.Success = true

}

func (rf *Raft) appendEntriesToPeer(idx int) {
	for !rf.killed() {
		rf.mu.Lock()
		rf.Log("appendEntries(heartbeat)")
		if rf.role != LEADER {
			rf.mu.Unlock()
			rf.setHeartBeatTime(idx)
			return
		}

		args := AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
		}

		rf.setHeartBeatTime(idx)

		reply := AppendEntriesReply{}
		resCh := make(chan bool, 1)
		go func(args *AppendEntriesArgs, reply *AppendEntriesReply) {
			ok := rf.peers[idx].Call("Raft.AppendEntries", args, reply)
			if !ok {
				time.Sleep(time.Millisecond * 10)
			}
			resCh <- ok
		}(&args, &reply)

		rf.mu.Lock()
		if reply.Term > args.Term {
			rf.role = FOLLOWER
			rf.setElectionTime()
			rf.currentTerm = reply.Term
			rf.mu.Unlock()
			return
		} else {
			rf.mu.Unlock()
			return
		}
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term         int
	VotedGranted bool
}

func (rf *Raft) getLastTermIndex() (term, idx int) {
	// 2A时为0
	return 1, 1
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.Log("set out a Vote request.")
	reply.Term = rf.currentTerm
	reply.VotedGranted = false

	// 如果候选人的term比自己还小
	if args.Term < rf.currentTerm {
		return
	} else if args.Term == rf.currentTerm {
		if rf.role != LEADER && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
			rf.votedFor = args.CandidateId
			reply.VotedGranted = true
			rf.role = FOLLOWER
			rf.persist()
			return
		} else {
			return
		}
	}

	defer rf.persist()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		// 还需要满足term和index条件，最终才能投票
		rf.role = FOLLOWER
		rf.votedFor = -1
	}

	lastLogTerm, lastLogIndex := rf.getLastTermIndex()

	if lastLogTerm > args.LastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
		return
	}

	rf.currentTerm = args.Term
	rf.votedFor = args.CandidateId
	rf.role = FOLLOWER
	reply.VotedGranted = true
	// rf自己重新开启选举倒计时，需要维持心跳检测
	rf.setElectionTime()
	return
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	close(rf.killCh)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.Log("start Election...")
	if rf.role == LEADER {
		return
	}
	rf.setElectionTime()
	// 变换成 竞选者
	rf.role = CANDIDATE
	// 自增任期号
	rf.currentTerm++
	// 投自己
	rf.votedFor = rf.me

	lastLogTerm, lastLogIndex := rf.getLastTermIndex()

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	rf.mu.Unlock()

	votedCount, cnt := 1, 1

	votedCh := make(chan bool, len(rf.peers))

	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}

		// 投票不能等，因此放在go协程里面
		go func(ch chan bool, i int) {
			reply := RequestVoteReply{}
			rf.sendRequestVote(i, &args, &reply)
			ch <- reply.VotedGranted

			if reply.Term > args.Term {
				// 从竞选者变成跟随者
				if rf.currentTerm < reply.Term {
					rf.mu.Lock()
					rf.Log("from candidate to follower")
					rf.currentTerm = reply.Term
					rf.role = FOLLOWER
					rf.setElectionTime()
					rf.mu.Unlock()
				}
			}

		}(votedCh, idx)
	}

	for {
		r := <-votedCh
		cnt++
		if r == true {
			votedCount++
		}

		if len(rf.peers) == cnt || votedCount > len(rf.peers)/2 || (cnt-votedCount) > len(rf.peers)/2 {
			break
		}
	}

	if votedCount <= len(rf.peers)/2 {
		rf.Log("less than half...")
	}

	rf.mu.Lock()

	if rf.currentTerm == args.Term && rf.role == CANDIDATE {
		rf.role = LEADER
		rf.setElectionTime()
		// 还需要更新索引相关字段，2A暂时不涉及
	}

	if rf.role == LEADER {

	}
	rf.mu.Unlock()
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.killCh:
			return
		case <-rf.electionTimer.C:
			rf.startElection()
		}

	}
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh
	rf.killCh = make(chan struct{})
	rf.debugMode = true
	rf.role = FOLLOWER
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.logEntries = make([]LogEntry, 0)
	rf.appendEntriesTimers = make([]*time.Timer, len(rf.peers))
	for i, _ := range rf.peers {
		rf.appendEntriesTimers[i] = time.NewTimer(HEARTBEATTIMEOUT)
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	rf.setElectionTime()
	go rf.ticker()

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(idx int) {
			for {
				select {
				case <-rf.killCh:
					return
				case <-rf.appendEntriesTimers[idx].C:
					rf.appendEntriesToPeer(idx)
				}
			}
		}(i)
	}
	return rf
}
