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
import "time"
import "sort"
import "math/rand"
import "fmt"
import "bytes"
import "encoding/gob"

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

// Enum State Type for Raft Object
type StateType int
const (
	Follower StateType = 0
	Candidate StateType = 1
	Leader StateType = 2
)
// Debug Flag
const DEBUG = false
// Infty Constant
const INF   = 1047576

// Log Struct 
type Entry struct {
	Term int
    Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int
	// Your data here.
	// Assign I
	currentTerm	  		int
	votedFor			int
	state				StateType 
	heartbeat 			chan bool
	electLeader			chan bool
	changeState			chan StateType
	// Assign II
	log					[]Entry
	commitIndex			int
	commitChan			chan ApplyMsg
	nextIndex			[]int 
	matchIndex			[]int
	commitLog			chan bool 
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here.
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	w := new(bytes.Buffer)
    e := gob.NewEncoder(w)
    e.Encode(rf.currentTerm)
    e.Encode(rf.votedFor)
    e.Encode(rf.log)
    data := w.Bytes()
    rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	if data == nil || len(data) < 1 { return }

    rf.mu.Lock()
    r := bytes.NewBuffer(data)
    d := gob.NewDecoder(r)
    d.Decode(&rf.currentTerm)
    d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	rf.mu.Unlock()
}

/* Raft Main Thread */
func (rf *Raft) working() {
	heartDuration := time.Duration(150 + rand.Intn(150))
	electionDuration := time.Duration(300 + rand.Intn(150))
	leadDuration := time.Duration(100)
	if DEBUG { fmt.Printf("Duration: %d %d\n", heartDuration, electionDuration) }
	for{
		switch rf.state {
		case Follower:
			select{
			case s := <- rf.changeState: rf.tranState(s)
			case <-rf.heartbeat: 
				// if DEBUG { fmt.Printf("#%d HeartBeat\n", rf.me) }
			case <-time.After(heartDuration * time.Millisecond): 
				rf.mu.Lock()	
				rf.changeState <- Candidate
			}
		case Candidate:
			select{
			case <- rf.electLeader: rf.elecLeader()
			case s := <- rf.changeState: rf.tranState(s)
			case <- time.After(electionDuration * time.Millisecond): rf.electLeader <- true
			}
		case Leader: 
			select{
			case s := <- rf.changeState: rf.tranState(s)
			case <- time.After(leadDuration * time.Millisecond): rf.lead()
			}
		}
	}
}

func (rf *Raft) lead() {
	// Generate Sync Package for Followers
	rf.mu.Lock()
	request := make([]AppendEntriesArgs, len(rf.peers))
	nextIndex := make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			request[i].LeaderID = rf.me
			request[i].Term = rf.currentTerm
			request[i].PrevLogIndex = rf.nextIndex[i] - 1
			request[i].PrevLogTerm = rf.log[request[i].PrevLogIndex].Term
			request[i].LeaderCommit = rf.commitIndex
			request[i].Entries = rf.log[request[i].PrevLogIndex + 1:]
			nextIndex[i] = len(rf.log)
		}
	}
	rf.mu.Unlock()

	// Send Sync Package for Followers
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func (i int, rf *Raft)  {
				var reply AppendEntriesReply
				if rf.sendAppendEntries(i, request[i], &reply) {
					if reply.Term > rf.currentTerm { 
						rf.currentTerm = reply.Term
						rf.mu.Lock()
						rf.changeState <- Follower 
						return
					}
					if reply.Success {
						rf.nextIndex[i] = nextIndex[i]
						rf.matchIndex[i] = nextIndex[i] - 1
						rf.commitLog <- true
					}else{
						rf.nextIndex[i] = rf.nextIndex[i] - 1
					}
				}
			}(i, rf)
		}
	}
}

func (rf *Raft) committing(){
	for {
		select {
		case <- rf.commitLog:
			if rf.state == Leader{
				commitIndex := rf.commitIndex
				matchIndex := append([]int{}, rf.matchIndex...)
				sort.Ints(matchIndex)
				majorCommit := matchIndex[(len(rf.peers) - 1) / 2]

				for i := commitIndex + 1; i <= majorCommit; i++ {
					rf.commitChan <- ApplyMsg{Index: i, Command: rf.log[i].Command}
					rf.commitIndex = i
					if DEBUG { fmt.Printf("Leader #%d Commit Log $%d(%d, %d)\n", rf.me, i, rf.log[i].Term, rf.log[i].Command)}
				}
			}
		}
	}
}

func (rf *Raft) tranState(state StateType){
	var name = [3]string{"Follower", "Candidate", "Leader"}
	if DEBUG { fmt.Printf("#%d Change to %s\n", rf.me, name[state]) }
	rf.state = state
	for i := 0; i < len(rf.peers); i++ { rf.nextIndex[i] = 1 }
	rf.persist()
	switch state {
	case Follower:
	case Candidate:
		rf.electLeader <- true
	case Leader:
		for i := 0; i < len(rf.peers); i++ { rf.nextIndex[i] = len(rf.log) }
        rf.matchIndex[rf.me] = len(rf.log) - 1
	}
	rf.mu.Unlock()
}

func (rf *Raft) elecLeader(){
	// Update Term
	rf.currentTerm ++
	// Get votes from peers
	lastLog := rf.log[len(rf.log) - 1]
	votes, request := 1, RequestVoteArgs{rf.currentTerm, rf.me, len(rf.log), lastLog.Term}
	rf.votedFor = rf.me
	rf.persist()
	if DEBUG { fmt.Printf("#%d(%d) is Collecting Votes\n", rf.me, rf.currentTerm) }
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(i int, votes *int, term int, rf *Raft, major int){
				var reply RequestVoteReply
				if rf.sendRequestVote(i, request, &reply) && reply.VoteGranted {
					*votes++ 
					if DEBUG { fmt.Printf("#%d Got Vote from #%d (%d/%d)\n", rf.me, i, *votes, major) }
					if *votes == major && rf.state == Candidate && rf.currentTerm == term{
						rf.mu.Lock()
						rf.changeState <- Leader
						if DEBUG { fmt.Printf("#%d Become Leader\n", rf.me) }
					}
				}
			}(i, &votes, rf.currentTerm, rf, int(len(rf.peers) / 2) + 1)
		}
	}
}

//
// example RequestVote RPC arguments & RPC reply structure.
//
type RequestVoteArgs struct {
	// Your data here.
	// For Assign I
	TermID int
	CandidateID int
	// For Assign II
	LastLogIndex int
    LastLogTerm int
}
type RequestVoteReply struct {
	// Your data here.
	TermID int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Lower Priority
	if args.TermID < rf.currentTerm || (args.TermID == rf.currentTerm && rf.state == Leader){
		reply.TermID = rf.currentTerm
		reply.VoteGranted = false
		if DEBUG { fmt.Printf("#%d Reject #%d, Due to Legacy Term\n", rf.me, args.CandidateID) }
		return
	}
	// Higher Term
	if args.TermID > rf.currentTerm {
		rf.currentTerm = args.TermID
		rf.votedFor = -1
		if rf.state != Follower{
			rf.mu.Lock()
			rf.changeState <- Follower
		}
	}
	// No Vote
	if rf.votedFor != -1 { 
		reply.TermID = rf.currentTerm
		reply.VoteGranted = false 
		if DEBUG { fmt.Printf("#%d Reject #%d, Due to No Vote\n", rf.me, args.CandidateID) }
		return
	}
	// Log too Old
	lastLog := rf.log[len(rf.log)-1]
	if args.LastLogTerm < lastLog.Term || (args.LastLogTerm == lastLog.Term && args.LastLogIndex < len(rf.log)){
		reply.TermID = rf.currentTerm
		reply.VoteGranted = false 
		if DEBUG { fmt.Printf("#%d Reject #%d, Due to Legacy Log\n", rf.me, args.CandidateID) }
		return
	}
	// Vote for candidate
	if DEBUG { fmt.Printf("#%d Vote for #%d\n", rf.me, args.CandidateID) }
	reply.TermID = rf.currentTerm
	rf.votedFor = args.CandidateID
	reply.VoteGranted = true
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
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	// For Assign I
	Term			int
	LeaderID		int
	// For Assign II
	PrevLogIndex	int
	PrevLogTerm		int
	Entries			[]Entry
	LeaderCommit	int
}
type AppendEntriesReply struct {
	// For Assign I
	Term			int
	Success			bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// Reject legacy append entries requests
	if (args.Term < rf.currentTerm){
		reply.Term = rf.currentTerm
		reply.Success = false
		if DEBUG { fmt.Printf("Receive %d(%d) -> %d(%d) : Legacy\n", args.LeaderID, args.Term, rf.me, rf.currentTerm) }
		return
	}

	// Got a valid heart beat
	if rf.state == Follower { rf.heartbeat <- true }
	
	// Solve append entries requests
	if args.Term > rf.currentTerm{
		rf.currentTerm = args.Term
		rf.mu.Lock()
		rf.changeState <- Follower
	}

	// Log missing or log mismatching
	if len(rf.log) - 1 < args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		if DEBUG { fmt.Printf("Receive %d(%d) -> %d(%d) : Reject\n", args.LeaderID, args.Term, rf.me, rf.currentTerm) }
		return
	}

	// Update log
	reply.Term = rf.currentTerm
	reply.Success = true
	if len(args.Entries) > 0 {
		i := 0
		for ; i < len(args.Entries) && args.PrevLogIndex + i + 1 < len(rf.log) && rf.log[args.PrevLogIndex + i + 1].Term == args.Entries[i].Term; i++ { }
		if args.PrevLogIndex + i + 1 < rf.commitIndex {
			reply.Term = rf.currentTerm
			reply.Success = false
			return
		}
		rf.log = append(rf.log[: args.PrevLogIndex + i + 1], args.Entries[i :]...)
		if DEBUG { fmt.Printf("Receive %d(%d) -> %d(%d) : Append %d logs\n", args.LeaderID, args.Term, rf.me, rf.currentTerm, len(args.Entries) - i) }
	}
	rf.persist()
	// Apply commits
	if args.LeaderCommit > rf.commitIndex {
		for i := rf.commitIndex + 1; i <= args.LeaderCommit && i < len(rf.log); i++ {
			if i > 0 { rf.commitChan <- ApplyMsg{Index: i, Command: rf.log[i].Command} }
			if DEBUG { fmt.Printf("Follower #%d commit Log $%d(%d, %d)\n", rf.me, i, rf.log[i].Term, rf.log[i].Command) }
			rf.commitIndex = i
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	
	rf.mu.Lock()
	isLeader = (rf.state == Leader)
	if isLeader {
		term, index = rf.currentTerm, len(rf.log) 
		rf.log = append(rf.log, Entry{term, command})
		rf.nextIndex[rf.me]++
		rf.matchIndex[rf.me] = rf.nextIndex[rf.me] - 1
		rf.persist()
		if DEBUG { fmt.Printf("Leader #%d: Receive New Log $%d(%d)\n", rf.me, rf.matchIndex[rf.me], rf.log[rf.matchIndex[rf.me]].Command) }
	}
	rf.mu.Unlock()

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
	// Your initialization code here.
	// Assign I
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.state = Follower
	rf.heartbeat = make(chan bool, 1)
	rf.electLeader = make(chan bool, 1)
	rf.changeState = make(chan StateType, 1)
	// Assign II
	rf.commitLog = make(chan bool, INF)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ { rf.nextIndex[i] = 1 }
	for i := 0; i < len(rf.peers); i++ { rf.matchIndex[i] = 0 }
	rf.commitIndex = 0
	rf.log = append(rf.log, Entry{-1, rf.currentTerm})
	rf.commitChan = applyCh
	// Run Raft Thread
	go rf.working()
	go rf.committing()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}
