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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      any
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm       int           // 当前任期
	votedFor          int           // 投票给的节点
	log               []LogEntry    // 日志
	state             int           // 当前节点状态：跟随者、候选者、领导者
	applyCh           chan ApplyMsg // 应用通道
	lastAppliedID     int           // 已应用的 ID
	commitIndex       int           // 已提交的索引
	lastheartbeattime time.Time     // 最后一次收到心跳的时间
	nextIndex         []int         // 对每个 follower，下一个要发送的 log index
	matchIndex        []int         // 对每个 follower，已经匹配的最大 index
}

type LogEntry struct {
	Command any // 命令
	Term    int // 日志的任期
}

const (
	FOLLOWER  = iota // 跟随者
	CANDIDATE        // 候选者
	LEADER           // 领导者
)

const (
	UnCommitted = iota // 未提交
	Committed          // 已提交
	Applied            // 已应用
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int // 候选者的任期
	CandidateId  int // 候选者ID
	LastLogIndex int // 候选者日志最后一条的索引
	LastLogTerm  int // 候选者日志最后一条的任期
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int  // 响应的任期
	VoteGranted bool // 是否获得投票
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// 如果请求中的任期小于当前任期，拒绝投票
	if args.Term < rf.currentTerm {
		return
	}

	// 如果当前任期小于请求的任期，更新任期并转为跟随者
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
	}

	// 如果尚未投票，或者是已经投给CandidateId，则校验日志进行投票
	if args.Term == rf.currentTerm &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateId) {

		// 检查日志
		lastLogIndex := len(rf.log) - 1
		lastLogTerm := 0
		if lastLogIndex >= 0 {
			lastLogTerm = rf.log[lastLogIndex].Term
		}

		candidateLogUpToDate := (args.LastLogTerm > lastLogTerm) ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

		if candidateLogUpToDate {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
		}
	}

	rf.persist()
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

// 共用发送投票请求
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) startElection() {
	// 任期+1，状态为候选者
	rf.currentTerm++
	rf.state = CANDIDATE
	rf.votedFor = rf.me               // 投给自己
	rf.lastheartbeattime = time.Now() // 重置选举超时时间
	rf.persist()

	// 获取最后一条日志的索引和任期
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
	}

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	votesReceived := 1 // 记录收到的投票数
	var mu sync.Mutex

	// 向除了自己之外的所有节点发送投票请求
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(server int) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, args, reply)

			// 这里在发送完rpc请求后再加锁，防止在rpc之前加锁但是rpc卡住导致的死锁
			mu.Lock()
			defer mu.Unlock()

			if !ok {
				return
			}

			// 如果成功且收到投票，则记录
			if reply.VoteGranted {
				rf.mu.Lock()
				if args.Term == rf.currentTerm && rf.state == CANDIDATE {
					votesReceived++

					// 获得大多数的投票，转换成领导者
					if votesReceived > len(rf.peers)/2 {
						rf.state = LEADER
						rf.nextIndex = make([]int, len(rf.peers))
						rf.matchIndex = make([]int, len(rf.peers))

						for i := range rf.peers {
							rf.nextIndex[i] = len(rf.log)
							rf.matchIndex[i] = 0
						}

						// 初始化领导者的 matchIndex
						rf.matchIndex[rf.me] = len(rf.log) - 1
					}
				}
				rf.mu.Unlock()
			}

			// 如果收到新的任期大于当前任期，则转换成跟随者
			if reply.Term > rf.currentTerm {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = FOLLOWER
					rf.votedFor = -1
					rf.lastheartbeattime = time.Now()
					rf.persist()
				}
				rf.mu.Unlock()
				return
			}

		}(i)
	}
}

// 领导者向其他节点发送 AppendEntries 请求，server代表跟随者节点
func (rf *Raft) sendAppendEntriesToPeer(server int, term int) {
	rf.mu.Lock()

	// 二次检查状态，不是领导者直接退出
	if rf.state != LEADER || term != rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	// 确保 nextIndex 不越界
	if rf.nextIndex[server] > len(rf.log) {
		rf.nextIndex[server] = len(rf.log)
	}

	nextIdx := rf.nextIndex[server]
	prevLogIndex := nextIdx - 1

	// 获取 prevLogTerm
	prevLogTerm := 0
	if prevLogIndex >= 0 && prevLogIndex < len(rf.log) {
		prevLogTerm = rf.log[prevLogIndex].Term
	}

	// 构建 entries 切片
	var entries []LogEntry
	if nextIdx < len(rf.log) {
		entries = append([]LogEntry{}, rf.log[nextIdx:]...)
	} else {
		entries = []LogEntry{}
	}

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}

	rf.mu.Unlock()

	// 在发送rpc请求时，需要先解锁，避免死锁
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, &args, &reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 再次检查状态
	if rf.state != LEADER || term != rf.currentTerm {
		return
	}

	// term 过期
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.persist()
		return
	}

	if reply.Success {
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else {
		if reply.ConflictIndex != -1 {
			rf.nextIndex[server] = reply.ConflictIndex
		} else {
			rf.nextIndex[server]--
		}

		if rf.nextIndex[server] < 1 {
			rf.nextIndex[server] = 1
		}
	}

	// 更新 commitIndex
	rf.updateCommitIndex()
}

// 更新 commitIndex
func (rf *Raft) updateCommitIndex() {
	for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
		count := 1

		// 统计日志索引 N 是否被大多数节点提交
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= N {
				count++
			}
		}

		// 如果大多数节点都已提交到 N 索引位置，且 N 索引位置的日志任期与当前任期相同，则推进 commitIndex
		if count > len(rf.peers)/2 && rf.log[N].Term == rf.currentTerm {
			rf.commitIndex = N
			go rf.applyLogs()
			break
		}
	}
}

// 日志进行应用
func (rf *Raft) applyLogs() {
	for {
		rf.mu.Lock()

		// 如果 lastAppliedID 已经大于等于 commitIndex，则没有日志需要应用
		if rf.lastAppliedID >= rf.commitIndex {
			rf.mu.Unlock()
			return
		}

		rf.lastAppliedID++
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastAppliedID].Command,
			CommandIndex: rf.lastAppliedID,
		}

		rf.mu.Unlock()

		rf.applyCh <- msg
	}
}

// 领导者发送日志请求结构体
// AppendEntriesreq
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry // 日志条目
	LeaderCommit int        // Leader 的“已提交”状态的日志条目索引号
}

// AppendEntriesReply
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int // 表示发生缺失的日志索引
	ConflictTerm  int // 缺失日志的“上一个有效日志条目”的任期号
}
// 领导者发送日志请求
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 在3A中只需要实现心跳功能即可，3B中需要实现日志复制
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	// 初始化
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	// term 检查
	if args.Term < rf.currentTerm {
		return
	}

	// 更新为 follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
	}

	rf.lastheartbeattime = time.Now()

	// prevLogIndex 不存在，返回缺失的日志索引
	if args.PrevLogIndex >= len(rf.log) {
		reply.ConflictIndex = len(rf.log)
		return
	}

	// term 不匹配，寻找错误日志的索引和任期
	if args.PrevLogIndex >= 0 &&
		rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {

		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term

		i := args.PrevLogIndex
		for i > 0 && rf.log[i-1].Term == reply.ConflictTerm {
			i--
		}
		reply.ConflictIndex = i
		return
	}

	// append entries

	index := args.PrevLogIndex + 1 // 跟随者开始对齐的位置
	i := 0                         // 领导者发送的日志条目下标

	// 寻找冲突日志的索引位置
	for ; i < len(args.Entries); i++ {
		if index+i < len(rf.log) {
			if rf.log[index+i].Term != args.Entries[i].Term {
				// 如果存在任期不匹配的日志，进行截断
				rf.log = rf.log[:index+i]
				break
			}
		} else {
			break
		}
	}

	// 将剩余的日志条目添加到跟随者日志中
	if i < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[i:]...)
	}

	// 更新 commitIndex 和 lastAppliedID 索引并应用日志
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		go rf.applyLogs()
	}

	reply.Success = true
	rf.persist()
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
	defer rf.mu.Unlock()

	if rf.state != LEADER {
		return -1, rf.currentTerm, false
	}

	index := len(rf.log)
	term := rf.currentTerm

	rf.log = append(rf.log, LogEntry{
		Command: command,
		Term:    term,
	})

	// leader 自己的 matchIndex 必须更新
	rf.matchIndex[rf.me] = len(rf.log) - 1

	rf.persist()

	// 立即尝试 commit
	// rf.updateCommitIndex()

	// 触发复制
	go rf.broadcastAppendEntries()

	return index, term, true
}

// 触发复制
func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	term := rf.currentTerm
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendAppendEntriesToPeer(i, term)
	}
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

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		rf.mu.Lock()

		// Your code here (3A)
		// Check if a leader election should be started.

		// 只有 跟随者 和 候选者 才需要检查选举超时
		if rf.state == FOLLOWER || rf.state == CANDIDATE {
			electionTimeout := 150 + rand.Int63n(150) // 150-300ms 随机超时
			if time.Since(rf.lastheartbeattime) > time.Duration(electionTimeout)*time.Millisecond {
				// 超时，开始选举
				rf.startElection()
				// startElection 会重置 electionStart，所以这里不需要重复设置
			}
		}

		rf.mu.Unlock()

	}
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

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = FOLLOWER // 初始状态都为跟随者
	rf.lastheartbeattime = time.Now()
	rf.applyCh = applyCh
	rf.lastAppliedID = 0 // 已应用的索引，从0开始（表示还没有应用任何日志）
	rf.commitIndex = 0   // 已提交的索引，从0开始（表示还没有提交任何日志）
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{Term: 0}
	// 初始化领导者状态
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log) // 初始下一个要发送的索引是当前日志长度
		rf.matchIndex[i] = 0          // 初始已匹配的索引是0
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 计时器守护进程，检测到超时后，开始选举，投自己
	go rf.ticker()
	go rf.heartbeatLoop()

	return rf
}

// 心跳循环
func (rf *Raft) heartbeatLoop() {
	for !rf.killed() {
		time.Sleep(100 * time.Millisecond)

		rf.mu.Lock()
		if rf.state == LEADER {
			term := rf.currentTerm

			// 持续推进 commitIndex
			rf.updateCommitIndex()

			rf.mu.Unlock()

			for i := range rf.peers {
				if i != rf.me {
					go rf.sendAppendEntriesToPeer(i, term)
				}
			}
		} else {
			rf.mu.Unlock()
		}
	}
}
