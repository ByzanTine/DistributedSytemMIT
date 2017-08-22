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
import "labrpc"
import "log"
import "time"

// import "bytes"
// import "encoding/gob"

type ServerIdentity int

const (
	FOLLOWER ServerIdentity = 1 + iota
	CANDIDATE
	LEADER
)

type LogEntry struct {
	Term int
	Cmd  interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm          int
	votedFor             int // -1 indicate null
	identity             ServerIdentity
	lastHealthyTimeStamp time.Time
	randomTimeout        time.Duration
	votes                int

	// Used when the server is killed.
	done chan int

	// Use for logs.
	log         []LogEntry
	nextIndex   []int
	matchIndex  []int
	commitIndex int
	lastApplied int

	applyCommit chan int
}

func (rf *Raft) getLastLogIndex() int {
	if rf.log == nil {
		return 0
	}
	return len(rf.log)
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 0 {
		return -1
	}
	return rf.log[len(rf.log)-1].Term
}

// Must be called when term is updated.
func (rf *Raft) ResetState(newTerm int) {
	rf.currentTerm = newTerm
	rf.votedFor = -1
	rf.identity = FOLLOWER
	// rf.lastHealthyTimeStamp = time.Now()
	if newTerm > 0 {
		log.Print("Me: ", rf.me, " recv higher term request, change to follower")
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isleader = rf.identity == LEADER
	term = rf.currentTerm
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Reject before reset.
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.ResetState(args.Term)
	}

	if rf.votedFor != -1 {
		reply.VoteGranted = false
		return
	}
	if args.LastLogTerm < rf.getLastLogTerm() {
		reply.VoteGranted = false
		return
	}
	// Now args term is at least higher than replicated log term.
	if args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex < rf.getLastLogIndex() {
		reply.VoteGranted = false
		return
	}
	// Now either log term is higher or log index is higher if it's same term.

	// granted the vote otherwise
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	reply.Term = rf.currentTerm
	log.Print("Me: ", rf.me, " send Vote: ", rf.votedFor)
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

// AppendEntries handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastHealthyTimeStamp = time.Now()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	// Parse the prev log info. Return false when prev log didn't match.
	if rf.getLastLogIndex() < args.PrevLogIndex {
		log.Print("Me: ", rf.me, " index doesn't match args: ", args.PrevLogIndex, " vs rf.li:", rf.getLastLogIndex())
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if args.PrevLogIndex > 0 && (rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm) {
		log.Print("Me: ", rf.me, " term does't match, remove this one and followings")
		if args.PrevLogIndex == 1 {
			rf.log = nil
		} else {
			rf.log = rf.log[:args.PrevLogIndex-1]
		}
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.ResetState(args.Term)
	}
	// Your code here (2A, 2B).
	rf.identity = FOLLOWER
	for i, entry := range args.Entries {
		// If new entry already has a place, delete the existing entries.
		// TODO(yejiayu): refactor this part log accessor.
		realLogIndex := args.PrevLogIndex + i
		if args.PrevLogIndex > 0 &&
			realLogIndex < len(rf.log) &&
			rf.log[realLogIndex].Term != entry.Term {
			// Remove the ones that follow
			log.Print("Me: ", rf.me, " entry doesn't match, remove from ", realLogIndex+1)
			rf.log = rf.log[:args.PrevLogIndex]
		} else {
			if realLogIndex < len(rf.log) && rf.log[realLogIndex].Cmd != entry.Cmd {
				log.Fatal("Protocol failure: command mismatch in same term")
			}
		}
		rf.log = append(rf.log, entry)
	}
	if args.LeaderCommitIndex > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommitIndex, rf.getLastLogIndex())
	}
	if len(args.Entries) > 0 {
		log.Print("Me: ", rf.me, " recv AppendEntries: ", args)
	}
	reply.Term = rf.currentTerm
	reply.Success = true
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = rf.identity == LEADER
	term = rf.currentTerm
	if !isLeader {
		return index, term, isLeader
	}
	rf.log = append(rf.log, LogEntry{Cmd: command, Term: term})
	index = rf.getLastLogIndex()
	log.Print("Me: ", rf.me, " Write entry to local log: ", rf.log[len(rf.log)-1])

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	close(rf.done)
}

func (rf *Raft) sendRequestVoteAndReport(index int) {
	rf.mu.Lock()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	rf.mu.Unlock()

	var reply RequestVoteReply
	ok := rf.sendRequestVote(index, &args, &reply)
	votes := 0
	if !ok {
		// log.Print("Me: ", rf.me, " SendRequestVote Failed")
		return
	}
	if reply.VoteGranted == true {
		votes = 1
	}
	rf.mu.Lock()
	rf.votes += votes
	rf.mu.Unlock()
}

// Must not be called while holding rf.mu
func (rf *Raft) startElection(me int) {
	rf.identity = CANDIDATE
	rf.votedFor = me
	rf.lastHealthyTimeStamp = time.Now()
	rf.currentTerm++
	rf.votes = 1 // self vote
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
	}
	log.Print("FOLLOWER Me: ", me, " electionTimer timeouts, term: ", rf.currentTerm)
	for index := range rf.peers {
		if index != me {
			go rf.sendRequestVoteAndReport(index)
		}
	}
	rf.randomTimeout = getElectionTimeout()

}

func (rf *Raft) handleElectionTicker(me int, electionTicker *time.Ticker) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Timeout.
	switch id := rf.identity; id {
	case FOLLOWER:
		if time.Now().Sub(rf.lastHealthyTimeStamp) > rf.randomTimeout {
			rf.startElection(me)
		}
	case CANDIDATE:
		if time.Now().Sub(rf.lastHealthyTimeStamp) > rf.randomTimeout {
			log.Print("CANDIDATE Me: ", me, " electionTimer timeouts, restart election")
			// Restart election.
			rf.startElection(me)
			return
		}
		if rf.votes > len(rf.peers)/2 {
			log.Print("CANDIDATE me: ", me, " become LEADER")
			rf.identity = LEADER
		}
	case LEADER:
		// log.Print("LEADER Me: ", me, " electionTimer timeouts, stop ticker.")
		// electionTicker.Stop()
	}
}

func (rf *Raft) processReply(reply *AppendEntriesReply, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.identity == LEADER {
		if reply.Success {
			rf.nextIndex[index] = rf.getLastLogIndex() + 1
			rf.matchIndex[index] = rf.getLastLogIndex()
		}
		if (reply.Term == rf.currentTerm) && !reply.Success {
			// need to find if (only b/c of log inconsistency)
			log.Print("Me: ", rf.me, " can't find the existing entry, decrement nextIndex for ", index)
			rf.nextIndex[index]--
		}
	}
	if reply.Term > rf.currentTerm && !reply.Success {
		rf.ResetState(reply.Term)
	}
}

// Index is the peer index.
func (rf *Raft) appendPrevLogInfo(args *AppendEntriesArgs, index int) {
	// If just sending a heartbeat, send the most recent log entry.
	if len(args.Entries) == 0 {
		args.PrevLogIndex = rf.getLastLogIndex()
		args.PrevLogTerm = rf.getLastLogTerm()
	} else {
		if rf.nextIndex[index] == 1 {
			// A new entry.
			args.PrevLogIndex = 0
			args.PrevLogTerm = -1
		} else {
			args.PrevLogIndex = rf.nextIndex[index] - 1
			args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
		}

	}
}

func (rf *Raft) handleHeartbeatTicker(me int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.identity == LEADER {
		for index := range rf.peers {
			if index != me {
				args := AppendEntriesArgs{
					Term:              rf.currentTerm,
					Entries:           nil,
					LeaderCommitIndex: rf.commitIndex,
				}
				if rf.getLastLogIndex() >= rf.nextIndex[index] {
					args.Entries = make([]LogEntry, rf.getLastLogIndex()-rf.nextIndex[index]+1)
					copy(args.Entries, rf.log[rf.nextIndex[index]-1:])
				}
				rf.appendPrevLogInfo(&args, index)
				go func(index int, args AppendEntriesArgs) {
					var reply AppendEntriesReply
					ok := rf.peers[index].Call("Raft.AppendEntries", &args, &reply)
					if !ok {
						// log.Print("Me: ", me, " error sending AppendEntries to", index)
						return
					}
					rf.processReply(&reply, index)
				}(index, args)
			}
		}

		// Update commit index in leader's op.
		rf.matchIndex[me] = rf.getLastLogIndex()
		if getMajorityMax(rf.matchIndex) > rf.commitIndex {
			rf.commitIndex = getMajorityMax(rf.matchIndex)
			log.Print("Me: ", me, " update commitIndex: ", rf.commitIndex)
		}

	}
	if rf.commitIndex > rf.lastApplied {
		// Optimization.
		for i := 0; i < rf.commitIndex-rf.lastApplied; i++ {
			go func() {
				rf.applyCommit <- 1
			}()
		}
	}
}

func (rf *Raft) handleApply(me int, applyCh chan ApplyMsg) {
	for {
		select {
		case <-rf.applyCommit:
			rf.mu.Lock()
			// Ignore the redundant chan signal.
			if rf.lastApplied >= rf.getLastLogIndex() {
				rf.mu.Unlock()
				break
			}
			msg := ApplyMsg{Index: rf.lastApplied + 1, Command: rf.log[rf.lastApplied].Cmd}
			log.Print("Me: ", me, " apply msg: ", msg)
			rf.lastApplied++
			rf.mu.Unlock()
			applyCh <- msg
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.ResetState(0)
	rf.done = make(chan int)
	rf.randomTimeout = getElectionTimeout()

	rf.nextIndex = make([]int, len(rf.peers), len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers), len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1 // start with no log
		rf.matchIndex[i] = 0
	}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCommit = make(chan int)
	ticker := time.NewTicker(100 * time.Millisecond)
	electionTicker := time.NewTicker(50 * time.Millisecond)
	go func() {
		rf.handleApply(me, applyCh)
	}()
	go func() {
		for {
			select {
			case <-electionTicker.C:
				// busy checking the health.
				rf.handleElectionTicker(me, electionTicker)
			case <-ticker.C:
				rf.handleHeartbeatTicker(me)
			case <-rf.done:
				return
			}

		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
