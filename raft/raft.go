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
	Match, Next uint64
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
	// temporary for debug
	debug bool
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	// init raftLog
	raftLog := &RaftLog{
		storage:         c.Storage,
		committed:       0,
		applied:         0,
		stabled:         0,
		entries:         nil,
		pendingSnapshot: nil,
		first:           0,
	}
	// init peers
	prs := make(map[uint64]*Progress)
	for _, p := range c.peers {
		prs[p] = &Progress{
			Match: 0,
			Next:  1,
		}
	}
	prs[c.ID] = &Progress{
		Match: 0,
		Next:  1,
	}

	return &Raft{
		id:               c.ID,
		Term:             0,
		Vote:             None,
		RaftLog:          raftLog,
		Prs:              prs,
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		msgs:             nil,
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
		//leadTransferee:   0, // TODO:
		//PendingConfIndex: 0, // TODO:
		//debug: true,
		debug: false,
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	index := r.RaftLog.LastIndex()
	logTerm, err := r.RaftLog.Term(index)
	if err != nil {
		return false
	}
	entries := r.RaftLog.getFollowingEntries(r.Prs[to].Next)

	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: logTerm, // refers to prevLogTerm in paper
		Index:   index,   // refers to prevLogIndex in paper
		Entries: entries,
		Commit:  r.RaftLog.committed,
	}
	r.sendMsg(msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}
	r.sendMsg(msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.electionElapsed++
	r.heartbeatElapsed++
	// actions
	switch r.State {
	case StateLeader:
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			if r.debug {
				log.Infof("%d broadcast heartbeat", r.id)
			}
			r.heartbeatElapsed = 0
			r.broadcastHeartbeat()
		}
	case StateCandidate:
		if r.electionElapsed >= r.electionTimeout { // reset
			if r.debug {
				log.Infof("%d election timeout", r.id)
			}
			err := r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
				To:      r.id,
				From:    r.id,
			})
			if err != nil {
				log.Panic("step msg error")
				return
			}
		}
	case StateFollower:
		if r.electionElapsed >= r.electionTimeout { // reset
			err := r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
				To:      r.id,
				From:    r.id,
			})
			if err != nil {
				log.Panic("step msg error")
				return
			}
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	if r.debug {
		log.Infof("%d become follower of %d, term:%d", r.id, lead, term)
	}
	r.State = StateFollower
	r.Lead = lead
	r.Term = term
	r.Vote = None
	r.resetVotes()
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term++
	if r.debug {
		log.Infof("%d become candidate term:%d", r.id, r.Term)
	}
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}
	r.State = StateCandidate
	r.electionElapsed = 0
	r.resetElectionTimer()
	r.Vote = r.id // vote for self
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if r.debug {
		log.Infof("%d become leader term:%d", r.id, r.Term)
	}
	r.State = StateLeader
	r.Vote = r.id
	r.resetVotes()
	index := uint64(0)
	if len(r.RaftLog.entries) > 0 {
		index = r.RaftLog.LastIndex() + 1
	}
	r.appendEntry(pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Term:      r.Term,
		Index:     index,
		Data:      nil, // noop entry
	})
	r.broadcastAppend()
	//r.broadcastHeartbeat() // FIXME
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if r.debug {
		log.Infof("\t\t%d step msg:%v", r.id, m)
	}
	// check local message
	if m.Term == 0 {
		switch m.MsgType {
		case pb.MessageType_MsgHup: // local message
			r.handleHup(m)
		case pb.MessageType_MsgBeat: // local message
			r.handleBeat(m)
		case pb.MessageType_MsgPropose: // local message
			return r.handlePropose(m)
		}
	}
	// FIXME: update lastApplied
	// check Term
	if m.Term < r.Term {
		if m.MsgType == pb.MessageType_MsgAppend {
			r.sendMsg(pb.Message{
				MsgType: pb.MessageType_MsgAppendResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
				Reject:  true,
			})
		}
		return nil
	}
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
	}
	// handle messages from peers
	// now m.Term == r.Term
	switch m.MsgType {
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgSnapshot:
		// TODO:
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgTransferLeader:
		// TODO:
	case pb.MessageType_MsgTimeoutNow:
		// TODO:
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// match prevLogIndex and prevLogTerm
	if r.debug {
		log.Infof("%d handle append entries from %d, ents:%v, r.ents:%v", r.id, m.From, m.Entries, r.RaftLog.entries)
	}
	prevLogTerm, err := r.RaftLog.Term(m.Index)
	if err != nil || prevLogTerm != m.LogTerm { // does not contain an entry with same index and term, send reject response
		r.sendMsg(pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			// FIXME: not sure whether contains
			//LogTerm:              ,
			//Index:                0,
			//Commit:               r.RaftLog.committed,
			Reject: true,
		})
	}
	r.becomeFollower(m.Term, m.From)
	// reset election timer // FIXME: not sure if necessary
	r.electionElapsed = 0
	// check conflicts(same index but different terms)
	startIndex := len(m.Entries)
	for i, en := range m.Entries {
		term, err := r.RaftLog.Term(en.Index)
		if err != nil || len(r.RaftLog.entries) == 0 { // index not exists
			startIndex = i
			break
		}
		if term != en.Term { // conflict, delete all that follow it
			err := r.RaftLog.deleteFollowingEntries(en.Index)
			if err != nil {
				log.Infof("delete entries err")
				return
			}
			startIndex = i
			break
		}
	}
	// append new entries
	if startIndex < len(m.Entries) {
		for ; startIndex < len(m.Entries); startIndex++ {
			r.appendEntry(*m.Entries[startIndex])
		}
	}
	// update committed
	if m.Commit > r.RaftLog.committed { // FIXME: not fully understand this rule in paper
		r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) { // FIXME: not sure
	// Your Code Here (2A).
	if r.debug {
		log.Infof("%d handle heartbeat from %d", r.id, m.From)
	}
	r.becomeFollower(m.Term, m.From)
	r.electionElapsed = 0 // reset election timer
	index := r.RaftLog.LastIndex()
	r.sendMsg(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		//LogTerm:              logTerm,
		Index:  index,
		Commit: r.RaftLog.committed,
	})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) broadcastHeartbeat() {
	for p := range r.Prs {
		if p != r.id {
			r.sendHeartbeat(p)
		}
	}
}

func (r *Raft) sendRequestVote(to uint64) {
	index := r.RaftLog.LastIndex()
	logTerm, err := r.RaftLog.Term(index)
	if err != nil {
		return
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: logTerm,
		Index:   index,
		Commit:  r.RaftLog.committed,
	}
	if r.debug {
		log.Infof("%d send req vote to %d, msg:%v", r.id, to, msg)
	}
	r.sendMsg(msg)
}

func (r *Raft) broadcastRequestVote() {
	if r.debug {
		log.Infof("%d broadcast req vote", r.id)
	}
	for p := range r.Prs {
		if p != r.id {
			r.sendRequestVote(p)
		}
	}
}

func (r *Raft) broadcastAppend() {
	for p := range r.Prs {
		if p != r.id {
			r.sendAppend(p)
		}
	}
}

func (r *Raft) sendMsg(m pb.Message) {
	r.msgs = append(r.msgs, m)
}

func (r *Raft) handleHup(m pb.Message) {
	if r.State == StateLeader || m.From != r.id { // skip
		return
	}
	if r.debug {
		log.Infof("%d handle hup", r.id)
	}
	r.becomeCandidate()
	r.broadcastRequestVote()
}

func (r *Raft) handleRequestVote(m pb.Message) {
	reject := true
	if r.State == StateFollower && (r.Vote == None || r.Vote == m.From) {
		lastIndex := r.RaftLog.LastIndex()
		logTerm, _ := r.RaftLog.Term(lastIndex)
		if m.Term > logTerm || (m.Term == logTerm && m.Index > lastIndex) { // at least up-to-date, vote
			r.Vote = m.From
			reject = false
		}
	}
	if r.debug {
		log.Infof("%d handle req vote from %d, reject:%v", r.id, m.From, reject)
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	}
	r.sendMsg(msg)
}

func (r *Raft) appendEntry(en pb.Entry) bool { // FIXME:
	return r.RaftLog.appendEntry(en)
}

func (r *Raft) appendEntries(ens []*pb.Entry) bool {
	for _, en := range ens {
		r.appendEntry(*en)
	}
	return true
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	// count votes
	if r.State != StateCandidate {
		return
	}
	r.votes[m.From] = !m.Reject
	if r.debug {
		log.Infof("%v handle req vote resp from %v, reject:%v, votes:%v", r.id, m.From, m.Reject, r.votes)
	}
	//if !m.Reject {
	cnt := 0
	rejectCnt := 0
	for _, v := range r.votes {
		if v {
			cnt++
		} else {
			rejectCnt++
		}
	}
	//log.Infof("cnt:%v, rejectcnt:%v, len:%v", cnt, rejectCnt, len(r.Prs))
	if 2*cnt > len(r.Prs) { // receive votes from the majority, convert to leader
		r.becomeLeader()
		// clear votes for future campaign
	}
	if 2*rejectCnt > len(r.Prs) {
		r.becomeFollower(r.Term, None)
	}
	//}
}

func (r *Raft) resetVotes() {
	r.votes = make(map[uint64]bool)
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if r.State == StateLeader {
		// update follower's log
		if m.Index < r.RaftLog.LastIndex() { // FIXME: not sure
			r.sendAppend(m.From)
		}
	}
}

func (r *Raft) handleBeat(m pb.Message) {
	if r.State == StateLeader {
		r.broadcastHeartbeat()
	}
}

func (r *Raft) handlePropose(m pb.Message) error {
	if r.State == StateLeader {
		var entries []*pb.Entry
		for _, en := range m.Entries {
			entries = append(entries, &pb.Entry{
				EntryType: en.EntryType,
				Term:      r.Term,
				Index:     r.RaftLog.LastIndex(),
				Data:      en.Data,
			})
		}
		r.appendEntries(entries)
	}
	return ErrProposalDropped
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	if r.State != StateLeader {
		return
	}
	// update progress
	// TODO:
	r.Prs[m.From].Match = m.Commit
	r.Prs[m.From].Next = m.Commit + 1 // FIXME:

	r.updateCommitted()
}

func (r *Raft) resetElectionTimer() {
	r.electionElapsed = 0
	base := 10 * r.heartbeatTimeout
	r.electionTimeout = base + rand.Intn(base)
}

func (r *Raft) updateCommitted() {
	if r.RaftLog.committed == r.RaftLog.LastIndex() { // updated
		return
	}
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
			r.RaftLog.committed = i
			// FIXME: maybe more action?
			break
		}
	}
}
