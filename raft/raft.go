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
	raftLog := newLog(c.Storage)
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

	hardState, _, err := c.Storage.InitialState() // FIXME: ConfState not used

	if err != nil {
		return nil
	}

	return &Raft{
		id:               c.ID,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
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
	entries := r.RaftLog.getFollowingEntries(r.Prs[to].Next)
	prevLogIndex := r.Prs[to].Match
	prevLogTerm, _ := r.RaftLog.Term(prevLogIndex)

	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Index:   prevLogIndex, // refers to prevLogIndex in paper
		LogTerm: prevLogTerm,  // refers to prevLogTerm in paper
		Entries: entries,
		Commit:  r.RaftLog.committed,
	}
	//if r.debug {
	//	log.Infof("%d send append to %d, msg:%v", r.id, to, msg)
	//}
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
	// init Progress of peers
	for _, p := range r.Prs {
		p.Match = r.RaftLog.LastIndex()
		p.Next = p.Match + 1
	}
	// commit noop entry
	index := r.RaftLog.LastIndex() + 1
	entries := []*pb.Entry{
		&pb.Entry{
			EntryType: pb.EntryType_EntryNormal,
			Term:      r.Term,
			Index:     index,
		},
	}
	err := r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		To:      r.id,
		From:    r.id,

		Entries: entries,
	})
	if err != nil {
		return
	}
	r.broadcastAppend()
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if r.debug {
		log.Infof("\t\t%d step msg:%v, r.ents:%v", r.id, m, r.RaftLog.entries)
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
	// skip prevLogIndex == 0 && prevLogTerm == 0, which means entries empty, should not reject
	if m.Index != 0 && (err != nil || prevLogTerm != m.LogTerm) { // does not contain an entry with same index and term, send reject response
		r.sendMsg(pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		})
		return
	}
	// now accept
	r.becomeFollower(m.Term, m.From)
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
			err := r.RaftLog.deleteFollowingEntries(en.Index)
			if err != nil {
				log.Infof("delete entries err")
				return
			}
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
		if r.debug {
			log.Infof("%d follower update m.committed:%v, r.lastIndex:%v, last:%d", r.id, m.Commit, r.RaftLog.LastIndex(), lastMatch)
		}
		r.RaftLog.setCommitted(min(m.Commit, min(r.RaftLog.LastIndex(), lastMatch)))
	}
	// respond to leader
	index := r.RaftLog.LastIndex()
	logTerm, _ := r.RaftLog.Term(index)
	r.sendMsg(pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		LogTerm: logTerm,
		Index:   index, // to update leader's match
	})
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
	logTerm, _ := r.RaftLog.Term(index)
	//logTerm, err := r.RaftLog.Term(index)
	//if err != nil {
	//	return
	//}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: logTerm, // lastLogTerm
		Index:   index,   // lastLogIndex
	}
	if r.debug {
		log.Infof("%d send req vote to %d, msg:%v, r.ents:%v", r.id, to, msg, r.RaftLog.entries)
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
	if r.debug {
		log.Infof("%d broadcast append", r.id)
	}
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
		log.Infof("%d handle hup, term:%v", r.id, r.Term)
	}
	r.becomeCandidate()
	r.broadcastRequestVote()
}

func (r *Raft) handleRequestVote(m pb.Message) {
	reject := true
	if r.State == StateFollower && (r.Vote == None || r.Vote == m.From) {
		lastIndex := r.RaftLog.LastIndex()
		lastLogTerm, _ := r.RaftLog.Term(lastIndex)
		if lastLogTerm < m.LogTerm || (lastLogTerm == m.LogTerm && lastIndex <= m.Index) { // at least up-to-date, vote
			r.Vote = m.From
			reject = false
		}
	}
	if r.debug {
		log.Infof("%d handle req vote from %d, reject:%v, ents:%v, m.ents:%v, r.vote:%d", r.id, m.From, reject, r.RaftLog.entries, m.Entries, r.Vote)
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
	r.broadcastAppend()
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
		if r.debug {
			log.Infof("%d handle propose", r.id)
		}
		var entries []*pb.Entry
		for _, en := range m.Entries {
			entries = append(entries, &pb.Entry{
				EntryType: en.EntryType,
				Term:      r.Term,
				Index:     r.RaftLog.LastIndex() + 1,
				Data:      en.Data,
			})
		}
		r.appendEntries(entries)
		r.updateCommitted()
		return nil
	}
	return ErrProposalDropped
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	if r.State != StateLeader {
		return
	}
	if m.Reject { // rejected by follower
		// decrement nextIndex and retries to append entry
		r.Prs[m.From].Next--
		r.Prs[m.From].Match = min(r.Prs[m.From].Match, r.Prs[m.From].Next-1) // FIXME: not sure
		r.sendAppend(m.From)
	} else { // accepted by follower
		// update progress
		r.Prs[m.From].Match = m.Index
		r.Prs[m.From].Next = m.Index + 1
		r.updateCommitted()
	}
	if r.debug {
		log.Infof("%d handle append resp from %d, index:%v, reject:%v", r.id, m.From, m.Index, m.Reject)
	}
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
	// update self for count
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
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
			r.RaftLog.setCommitted(i)
			//if r.debug {
			//	log.Infof("%d leader update r.committed:%v, r.last:%v", r.id, r.RaftLog.committed, r.RaftLog.LastIndex())
			//}
			r.broadcastAppend() // for followers to update committed
			break
		}
	}
}
