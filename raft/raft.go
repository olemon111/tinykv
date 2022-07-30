// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"github.com/pingcap-incubator/tinykv/log"
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

//const rdebug = true // for raft detail debug control
const rdebug = false

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next  uint64
	RecentActive bool // mark peer as active when receive heartbeat response, used for ignore sending too much snapshot when in partition
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int
	// used to record basic election timeout, so we can generate random timeout in [base, 2*base)
	baseElectionTimeout int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	// init raftLog with config
	raftLog := newLog(c.Storage)
	// init with hardState
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		return nil
	}
	// restart
	if c.Applied > 0 {
		raftLog.setApplied(c.Applied) // modify by config
		log.Infof("new raft applied:%v", c.Applied)
	}
	// init peers
	if c.peers == nil {
		c.peers = confState.Nodes
	}
	prs := make(map[uint64]*Progress)
	for _, p := range c.peers {
		prs[p] = &Progress{
			Match:        0,
			Next:         raftLog.LastIndex() + 1,
			RecentActive: true,
		}
	}
	return &Raft{
		id:                  c.ID,
		Term:                hardState.Term,
		Vote:                hardState.Vote,
		RaftLog:             raftLog,
		Prs:                 prs,
		State:               StateFollower,
		votes:               make(map[uint64]bool),
		msgs:                nil,
		Lead:                None,
		heartbeatTimeout:    c.HeartbeatTick,
		electionTimeout:     c.ElectionTick + rand.Intn(c.ElectionTick),
		baseElectionTimeout: c.ElectionTick,
		heartbeatElapsed:    0,
		electionElapsed:     0,
		//leadTransferee:   0, // TODO:
		//PendingConfIndex: 0, // TODO:
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	prevLogIndex := r.Prs[to].Next - 1 // index of log entry immediately preceding new ones
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	//if rdebug {
	//	log.Infof("%d sendappend to %d", r.id, to)
	//}
	if err != nil { // fall behind
		err := r.sendSnapshot(to)
		if err != nil {
			return false
		}
		return true
	}
	entries := r.RaftLog.getFollowingEntries(r.Prs[to].Next)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: prevLogTerm,  // refers to prevLogTerm in paper
		Index:   prevLogIndex, // refers to prevLogIndex in paper
		Entries: entries,
		Commit:  r.RaftLog.committed,
	}
	//log.Infof("%d send append to %d, msg:%v", r.id, to, msg)
	r.sendMsg(msg)
	return true
}

func (r *Raft) sendSnapshot(to uint64) error {
	if !r.Prs[to].RecentActive {
		log.Infof("%d ignore snapshot for peer %d is inactive", r.id, to)
		return errors.New("peer is inactive, ignore snapshot")
	}
	snapshot, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		return err // ErrSnapshotTemporarilyUnavailable, request again later
	}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgSnapshot,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		//Commit:   r.RaftLog.committed,
		Snapshot: &snapshot,
	}
	log.Infof("%d send snapshot to %d, msg:%v, meta.index:%d", r.id, to, msg, snapshot.Metadata.GetIndex())
	r.sendMsg(msg)
	// FIXME: maybe update match?
	r.Prs[to].Next = snapshot.Metadata.GetIndex() + 1
	return nil
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	if rdebug {
		//log.Infof("%d send heartbeat to %v", r.id, to)
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		//Commit:  r.RaftLog.committed,
	}
	r.sendMsg(msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	// actions
	//log.Infof("tick")
	switch r.State {
	case StateLeader:
		r.heartbeatTick()
		r.activeTick()
	case StateCandidate:
		r.electionTick()
	case StateFollower:
		r.electionTick()
	}
}

func (r *Raft) heartbeatTick() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		//log.Infof("%d heartbeat timeout", r.id)
		err := r.Step(pb.Message{
			MsgType: pb.MessageType_MsgBeat,
			To:      r.id,
			From:    r.id,
		})
		if err != nil {
			return
		}
	}
}

func (r *Raft) electionTick() {
	r.electionElapsed++
	if r.electionElapsed >= r.electionTimeout { // reset
		r.resetElectionTimer()
		err := r.Step(pb.Message{
			MsgType: pb.MessageType_MsgHup,
			To:      r.id,
			From:    r.id,
		})
		if err != nil {
			return
		}
	}
}

func (r *Raft) activeTick() {
	r.electionElapsed++
	if r.electionElapsed >= r.baseElectionTimeout { // leadership transfer should happen in baseElectionTimeout
		r.electionElapsed = 0
		r.leadTransferee = None
		// check leader quorum
		r.Prs[r.id].RecentActive = true // self active
		cnt := 0
		for p := range r.Prs {
			if r.Prs[p].RecentActive {
				cnt++
			}
			r.Prs[p].RecentActive = false // reset to inactive
		}
		if cnt < len(r.Prs)/2 { // old leader maybe in partition, so step down and stop to receive client requests
			log.Infof("%d step down maybe in partition, term:%v", r.id, r.Term)
			r.becomeFollower(r.Term, None)
			return
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	if rdebug {
		log.Infof("\t\t%d become follower of %d, term:%d", r.id, lead, term)
	}
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.Vote = None
	r.leadTransferee = None
	r.resetVotes()
	r.resetTimer()
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term++
	if rdebug {
		log.Infof("%d become candidate term:%d", r.id, r.Term)
	}
	r.Lead = None
	r.Vote = r.id // vote for self
	r.leadTransferee = None
	r.resetVotes()
	r.votes[r.id] = true
	r.resetTimer()
}

func (r *Raft) resetTimer() {
	r.heartbeatElapsed = 0
	r.resetElectionTimer()
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	if _, ok := r.Prs[r.id]; !ok {
		return
	}
	// reset to inactive
	for p := range r.Prs {
		r.Prs[p].RecentActive = true
	}
	// NOTE: Leader should propose a noop entry on its term
	log.Infof("%d become leader term:%d, votes:%v", r.id, r.Term, r.votes)
	r.State = StateLeader
	r.Lead = r.id
	r.Vote = None
	r.leadTransferee = None
	r.resetVotes()
	r.resetTimer()
	// init Progress of peers
	nextIndex := r.RaftLog.LastIndex() + 1
	for _, p := range r.Prs {
		p.Match = 0
		p.Next = nextIndex
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	// commit noop entry
	entries := []*pb.Entry{
		{
			EntryType: pb.EntryType_EntryNormal,
			Term:      r.Term,
			Index:     nextIndex,
		},
	}
	_ = r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		To:      r.id,
		From:    r.id,
		Entries: entries,
	})
	r.broadcastAppend()
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if rdebug {
		//log.Infof("\t\t%d step type:%v, from:%v, to:%v, term:%v, logterm:%v, index:%v, reject:%v", r.id, m.MsgType, m.From, m.To, m.Term, m.LogTerm, m.Index, m.Reject)
	}
	if _, ok := r.Prs[r.id]; !ok && len(r.Prs) != 0 {
		log.Infof("\t\t%d drop step type:%v, from:%v, to:%v, prs:%v", r.id, m.MsgType, m.From, m.To, r.Prs)
		return nil
	}
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	// now m.term <= r.term
	switch r.State {
	case StateLeader:
		return r.stepLeader(m)
	case StateCandidate:
		return r.stepCandidate(m)
	case StateFollower:
		return r.stepFollower(m)
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
	case pb.MessageType_MsgBeat: // local message
		r.handleBeat(m)
	case pb.MessageType_MsgPropose: // local message
		return r.handlePropose(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgTransferLeader: // local message
		r.handleTransferLeader(m)
	case pb.MessageType_MsgTimeoutNow:
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup: // local message
		r.handleHup(m)
	case pb.MessageType_MsgBeat: // local message
	case pb.MessageType_MsgPropose: // local message
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeader(m)
	case pb.MessageType_MsgTimeoutNow:
		r.handleTimeoutNow(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup: // local message
		r.handleHup(m)
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeader(m)
	case pb.MessageType_MsgTimeoutNow:
		r.handleTimeoutNow(m)
	}
	return nil
}

func (r *Raft) sendAppendResponse(to uint64, reject bool, index uint64) {
	if rdebug {
		log.Infof("%d send appendresponse to %d, reject:%v, term:%v, index:%d", r.id, to, reject, r.Term, index)
	}
	r.sendMsg(pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
		Index:   index,
	})
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		//log.Infof("%d reject append entries from %d, m.term:%d, r.term:%d", r.id, m.From, m.Term, r.Term)
		r.sendAppendResponse(m.From, true, None)
		return
	}
	// now accept
	r.resetElectionTimer()
	r.becomeFollower(m.Term, m.From)
	// match prevLogIndex and prevLogTerm
	//log.Infof("%d handle append entries from %d, ents:%v, r.ents:%v", r.id, m.From, m.Entries, r.RaftLog.entries)
	prevLogTerm, err := r.RaftLog.Term(m.Index)
	// skip prevLogIndex == 0 && prevLogTerm == 0, which means entries empty, should not reject
	if m.Index >= r.RaftLog.FirstIndex() && (err != nil || prevLogTerm != m.LogTerm) { // does not contain an entry with same index and term, send reject response
		//log.Infof("%d reject append entries from %d, m.term:%d, r.term:%d, m.index:%v, m.logTerm:%v, prevLogTerm:%v", r.id, m.From, m.Term, r.Term, m.Index, m.LogTerm, prevLogTerm)
		r.sendAppendResponse(m.From, true, None)
		return
	}
	//log.Infof("%d accept append entries from %d, m.term:%d, r.term:%d, m.ents:%v", r.id, m.From, m.Term, r.Term, m.Entries)
	//may delete with prevLogIndex && prevLogTerm // FIXME: updated to test case
	//_ = r.RaftLog.deleteFollowingEntries(m.Index + 1)
	// check conflicts(same index but different terms)
	lastMatch := m.Index // index of last matched entry
	startIndex := len(m.Entries)
	for i, en := range m.Entries {
		term, err := r.RaftLog.Term(en.Index)
		if err != nil || len(r.RaftLog.entries) == 0 { // index not exists
			startIndex = i
			break
		}
		if term != en.Term { // conflict, delete all that follow it
			_ = r.RaftLog.deleteFollowingEntries(en.Index)
			startIndex = i
			break
		}
		lastMatch = en.Index
	}
	// delete redundant entries if exists
	if startIndex >= 0 && startIndex < len(m.Entries) {
		_ = r.RaftLog.deleteFollowingEntries(m.Entries[startIndex].Index + 1)
	}
	// append new entries
	for ; startIndex < len(m.Entries); startIndex++ {
		r.appendEntry(*m.Entries[startIndex])
		lastMatch = m.Entries[startIndex].Index
	}
	// update committed
	if m.Commit > r.RaftLog.committed {
		if newCommitted := min(m.Commit, min(r.RaftLog.LastIndex(), lastMatch)); newCommitted > r.RaftLog.committed {
			r.RaftLog.setCommitted(newCommitted)
		}
	}
	// respond to leader
	index := r.RaftLog.LastIndex()
	r.sendAppendResponse(m.From, false, index) // index used to update leader's match
}

func (r *Raft) sendHeartbeatResponse(to uint64, reject bool, index uint64) {
	r.sendMsg(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Index:   index,
		Commit:  r.RaftLog.committed,
		Reject:  reject,
	})
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		if rdebug {
			//log.Infof("%d reject heartbeat from %d, m.term:%d, r.term:%d", r.id, m.From, m.Term, r.Term)
		}
		r.sendHeartbeatResponse(m.From, true, None)
		return
	}
	// accept
	r.resetElectionTimer()
	if rdebug {
		//log.Infof("%d accept heartbeat from %d, m.term:%d, r.term:%d", r.id, m.From, m.Term, r.Term)
	}
	r.becomeFollower(m.Term, m.From)
	index := r.RaftLog.LastIndex()
	r.sendHeartbeatResponse(m.From, false, index)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	log.Infof("%d handle snapshot, m:%v", r.id, m)
	metaData := m.Snapshot.Metadata
	// discard any existing snapshot with a smaller index
	if m.Term < r.Term || metaData.Index < r.RaftLog.committed {
		//r.sendAppendResponse(m.From, true, r.RaftLog.committed) // FIXME: not sure
		return
	}
	// now accept, reset state
	r.becomeFollower(m.Term, m.From)
	// update raftLog state
	r.RaftLog.resetState(metaData.Index)
	r.RaftLog.setPendingSnapshot(m.Snapshot) // save for ready
	// reset peers progress
	r.Prs = make(map[uint64]*Progress)
	if metaData.ConfState.Nodes != nil {
		for _, p := range metaData.ConfState.Nodes {
			r.Prs[p] = &Progress{}
		}
	}
	// update committed
	if m.Commit > r.RaftLog.committed {
		if newCommitted := min(m.Commit, r.RaftLog.LastIndex()); newCommitted > r.RaftLog.committed {
			r.RaftLog.setCommitted(newCommitted)
		}
	}
	// send response
	//r.sendAppendResponse(m.From, false, r.RaftLog.LastIndex())
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; ok {
		return
	}
	r.PendingConfIndex = None
	r.Prs[id] = &Progress{
		Match:        0,
		Next:         1, // fall behind, send snapshot to new node
		RecentActive: true,
	}
	if r.State == StateLeader {
		r.sendHeartbeat(id) // TODO: maybe useless, test later
		log.Infof("%v send heartbeat to init follower:%v", r.id, id)
		r.updateCommitted()
	}
	log.Infof("%d add node %d, prs:%v", r.id, id, r.Prs)
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	if id == r.id {
		r.Prs = make(map[uint64]*Progress)
		return
	}
	if _, ok := r.Prs[id]; !ok {
		return
	}
	r.PendingConfIndex = None
	delete(r.Prs, id)
	if r.State == StateLeader {
		r.updateCommitted()
	}
	log.Infof("%d remove node %d, prs:%v", r.id, id, r.Prs)
}

func (r *Raft) broadcastHeartbeat() {
	if rdebug {
		//log.Infof("%d broadcast heartbeat prs:%v", r.id, r.Prs)
	}
	for p := range r.Prs {
		if p != r.id {
			r.sendHeartbeat(p)
		}
	}
}

func (r *Raft) sendRequestVote(to uint64) {
	index := r.RaftLog.LastIndex()
	logTerm, _ := r.RaftLog.Term(index)

	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: logTerm, // lastLogTerm
		Index:   index,   // lastLogIndex
	}
	//log.Infof("%d send req vote to %d, msg:%v, r.ents:%v", r.id, to, msg, r.RaftLog.entries)
	r.sendMsg(msg)
}

func (r *Raft) broadcastRequestVote() {
	if rdebug {
		//log.Infof("%d broadcast req vote to prs:%v", r.id, r.Prs)
	}
	for p := range r.Prs {
		if p != r.id {
			r.sendRequestVote(p)
		}
	}
}

func (r *Raft) broadcastAppend() {
	if rdebug {
		log.Infof("%d broadcast append, term:%v", r.id, r.Term)
		if rdebug {
			for i, p := range r.Prs {
				log.Infof("prs: %v: active:%v, match:%v, next:%v", i, p.RecentActive, p.Match, p.Next)
			}
		}
	}
	for p := range r.Prs {
		if p != r.id {
			r.sendAppend(p)
		}
	}
}

func (r *Raft) sendMsg(m pb.Message) {
	if _, ok := r.Prs[m.To]; ok || len(r.Prs) == 0 || (m.MsgType == pb.MessageType_MsgAppendResponse && m.To == None) { // None used to pass TestHandleMessageType_MsgAppend2AB
		r.msgs = append(r.msgs, m)
	} else {
		log.Infof("send msg dropped, from:%v, to:%v, type:%s", m.From, m.To, m.MsgType)
	}
}

func (r *Raft) handleHup(m pb.Message) {
	if rdebug {
		log.Infof("%d handle hup, term:%v", r.id, r.Term)
	}
	if !(m.From == 0 || m.From == r.id) || len(r.Prs) == 0 {
		log.Infof("%v reject hup, m.from:%v, prs:%v", r.id, m.From, r.Prs)
		return
	}
	if !IsEmptySnap(r.RaftLog.pendingSnapshot) { // there's a leader sending snapshot
		log.Infof("pending snapshot, ignore hup")
		return
	}
	r.becomeCandidate()
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}
	r.broadcastRequestVote()
}

func (r *Raft) sendRequestVoteResponse(to uint64, reject bool) {
	if rdebug {
		log.Infof("%d send req vote resp to %d, reject:%v", r.id, to, reject)
	}
	r.sendMsg(pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	})
}

func (r *Raft) handleRequestVote(m pb.Message) {
	if m.Term < r.Term {
		r.sendRequestVoteResponse(m.From, true)
		return
	}
	reject := true
	if r.Vote == None || r.Vote == m.From {
		lastIndex := r.RaftLog.LastIndex()
		lastLogTerm, _ := r.RaftLog.Term(lastIndex)
		//log.Infof("lastIndex:%v, lastLogTerm:%v, m.index:%v, m.logTerm:%v", lastIndex, lastLogTerm, m.Index, m.LogTerm)
		if lastLogTerm < m.LogTerm || (lastLogTerm == m.LogTerm && lastIndex <= m.Index) { // at least up-to-date, vote
			r.Vote = m.From
			reject = false
		}
	}
	//log.Infof("%d handle req vote from %d, reject:%v, r.vote:%v, r.votes:%v, m.index:%v, m.logterm:%v, r.ents:%v", r.id, m.From, reject, r.Vote, r.votes, m.Index, m.LogTerm, r.RaftLog.entries)
	//if !reject {
	//	r.resetElectionTimer()
	//}
	r.sendRequestVoteResponse(m.From, reject)
}

func (r *Raft) appendEntry(en pb.Entry) bool {
	return r.RaftLog.appendEntry(en)
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	if m.Term != None && m.Term < r.Term {
		return
	}
	// count votes
	r.votes[m.From] = !m.Reject
	if rdebug {
		//log.Infof("%v handle req vote resp from %v, reject:%v, votes:%v", r.id, m.From, m.Reject, r.votes)
	}
	acceptCnt := 0
	rejectCnt := 0
	for _, v := range r.votes {
		if v {
			acceptCnt++
		} else {
			rejectCnt++
		}
	}
	if rdebug {
		//log.Infof("%d, cnt:%v, rejectcnt:%v, len:%v", r.id, acceptCnt, rejectCnt, len(r.Prs))
	}
	if 2*acceptCnt > len(r.Prs) { // receive votes from the majority, convert to leader
		r.becomeLeader()
	}
	// step down
	if 2*rejectCnt > len(r.Prs) {
		log.Infof("%v step down for reject votes", r.id)
		r.becomeFollower(r.Term, None)
	}
}

func (r *Raft) resetVotes() {
	r.votes = make(map[uint64]bool)
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	// mark as active
	r.Prs[m.From].RecentActive = true
	// update follower's log
	if m.Index < r.RaftLog.LastIndex() || m.Commit < r.RaftLog.committed {
		r.sendAppend(m.From)
	}
}

func (r *Raft) handleBeat(m pb.Message) {
	if m.From == r.id {
		r.broadcastHeartbeat()
	}
}

func (r *Raft) handlePropose(m pb.Message) error {
	if r.leadTransferee != None { // stop propose when transferring leader
		log.Infof("%v stop propose for transferring leader to %v", r.id, r.leadTransferee)
		return ErrProposalDropped
	}
	if rdebug {
		log.Infof("%d handle propose, ents:%v", r.id, m.Entries)
	}
	// append entries
	for _, entry := range m.Entries {
		if rdebug {
			//log.Infof("%v propose entry type:%s", r.id, entry.EntryType)
		}
		entry.Term = r.Term
		entry.Index = r.RaftLog.LastIndex() + 1
		// check confChange
		if entry.EntryType == pb.EntryType_EntryConfChange {
			if r.RaftLog.applied >= r.PendingConfIndex { // accept
				log.Infof("%v accept propose confchange applied:%v, pending:%v", r.id, r.RaftLog.applied, r.PendingConfIndex)
				r.PendingConfIndex = entry.Index
			} else { // reject: only one conf change may be pending
				log.Infof("%v reject propose confchange applied:%v, pending:%v", r.id, r.RaftLog.applied, r.PendingConfIndex)
				continue
			}
		}
		r.RaftLog.appendEntry(*entry)
	}
	// update self progress
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	r.broadcastAppend()
	r.updateCommitted()
	return nil
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	if m.Term != None && m.Term < r.Term {
		return
	}
	if m.Reject { // rejected by follower
		// decrement nextIndex and retries to append entry
		if r.Prs[m.From].Next > 1 {
			r.Prs[m.From].Next--
			r.sendAppend(m.From)
		}
	} else { // accepted by follower
		if r.leadTransferee == m.From {
			r.leadTransferee = None
			r.becomeFollower(m.Term, m.From)
			return
		}
		term, _ := r.RaftLog.Term(m.Index)
		// update progress
		if m.Index > r.Prs[m.From].Match {
			r.Prs[m.From].Match = m.Index
			r.Prs[m.From].Next = m.Index + 1
		}
		if term == r.Term {
			r.updateCommitted()
		}
	}
	//log.Infof("%d handle append resp from %d, index:%v, reject:%v", r.id, m.From, m.Index, m.Reject)
}

func (r *Raft) resetElectionTimer() {
	if rdebug {
		//log.Infof("%d reset electiontimeout %d", r.id, r.electionTimeout)
	}
	r.electionElapsed = 0
	r.electionTimeout = r.baseElectionTimeout + rand.Intn(r.baseElectionTimeout)
}

func (r *Raft) updateCommitted() {
	if rdebug {
		//log.Infof("%d before update committed prs:%v, first:%v, last:%v, applied:%v, committed:%v", r.id, r.Prs, r.RaftLog.FirstIndex(), r.RaftLog.LastIndex(), r.RaftLog.applied, r.RaftLog.committed)
	}
	if r.RaftLog.committed == r.RaftLog.LastIndex() { // updated
		return
	}
	// update self for count
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
	// match count
	for i := r.RaftLog.LastIndex(); i > r.RaftLog.committed; i-- {
		if t, _ := r.RaftLog.Term(i); t != r.Term { // only current term
			break
		}
		cnt := 0
		for p := range r.Prs {
			if r.Prs[p].Match >= i {
				cnt++
			}
		}
		if 2*cnt > len(r.Prs) { // beyond half committed
			if i != r.RaftLog.committed {
				log.Infof("%d leader update r.committed:%v to %v, r.last:%v, applied:%v", r.id, r.RaftLog.committed, i, r.RaftLog.LastIndex(), r.RaftLog.applied)
				r.RaftLog.setCommitted(i)
				r.broadcastAppend() // for followers to update committed
			}
			break
		}
	}
}

func (r *Raft) handleTransferLeader(m pb.Message) {
	// local message
	if m.Term != 0 {
		return
	}
	if r.State == StateLeader {
		// check the qualification of the transferee
		transferee := m.From
		if _, ok := r.Prs[transferee]; !ok || transferee == r.id || r.leadTransferee == transferee { // drop transfer to self
			return
		}
		r.leadTransferee = None // if already in transferring, abort the former
		log.Infof("%d handle transfer leader to %d", r.id, m.From)
		r.electionElapsed = 0                                 // wait in baseElectionTimeout
		r.leadTransferee = transferee                         // stop accepting new proposals by setting leadTransferee
		if r.Prs[transferee].Match == r.RaftLog.LastIndex() { // log up to date
			r.sendMsg(pb.Message{
				MsgType: pb.MessageType_MsgTimeoutNow,
				To:      transferee,
				From:    r.id,
				Term:    r.Term,
			})
		} else {
			r.sendAppend(transferee) // help the transferee
		}
	} else { // deliver to leader
		if r.Lead != None {
			m.To = r.Lead
			r.sendMsg(m)
		}
	}
}

func (r *Raft) handleTimeoutNow(m pb.Message) {
	if m.Term < r.Term {
		return
	}
	log.Infof("%d handle timeoutnow from %d", r.id, m.From)
	// start a new election immediately
	err := r.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
		From:    r.id,
		To:      r.id,
	})
	if err != nil {
		return
	}
}
