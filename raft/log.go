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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pkg/errors"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	// first index
	first uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	// TODO:
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		log.Panicf("storage.firstIndex error: %v", err)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		log.Panicf("storage.lastIndex error: %v", err)
	}
	//log.Infof("first:%v, last:%v", firstIndex, lastIndex)
	var entries []pb.Entry
	if firstIndex <= lastIndex {
		entries, _ = storage.Entries(firstIndex, lastIndex+1)
		//log.Infof("first:%d, last:%d, ents:%v", firstIndex, lastIndex, entries)
	}
	return &RaftLog{
		storage:         storage,
		committed:       firstIndex - 1,
		applied:         firstIndex - 1,
		stabled:         lastIndex,
		entries:         entries,
		pendingSnapshot: nil,
		first:           firstIndex,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	//log.Infof("unstable ents, ents:%v, stabled:%v, first:%v", l.entries, l.stabled, l.first)
	return l.entries[l.stabled-l.first+1:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	//log.Infof("ents:%v, first:%d, applied:%d, committed:%d", l.entries, l.first, l.applied, l.committed)
	return l.entries[l.applied-l.first+1 : l.committed-l.first+1]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	length := len(l.entries)
	if length > 0 {
		//log.Infof("last ent:%v, lastIndex:%v", l.entries[length-1], l.entries[length-1].Index)
		return l.entries[length-1].Index
	}
	return l.first - 1
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	// FIXME: not sure
	//log.Infof("i:%d, first:%d, len:%d", i, l.first, len(l.entries))
	if len(l.entries) == 0 {
		return 0, nil
	}
	if len(l.entries) > 0 && i >= l.first && i-l.first < uint64(len(l.entries)) { // in range
		term := l.entries[i-l.first].Term
		return term, nil
	}
	return 0, errors.New("entry not found")
}

//func (l *RaftLog) appendEntries(ents []pb.Entry) {
//	l.entries = append(l.entries, ents...)
//}

func (l *RaftLog) appendEntry(ent pb.Entry) bool {
	l.entries = append(l.entries, ent)
	return true
}

// delete entries after specific index (index included)
func (l *RaftLog) deleteFollowingEntries(index uint64) error {
	//log.Infof("delete ents:%v, index:%d, first:%d", l.entries, index, l.first)
	if index-l.first >= uint64(len(l.entries)) {
		err := errors.New("delete entries error")
		return err
	}
	l.entries = l.entries[:index-l.first] // TODO: not sure
	// update
	l.setStabled(min(l.stabled, l.LastIndex()))
	l.setCommitted(min(l.committed, l.LastIndex()))
	l.setApplied(min(l.committed, l.LastIndex()))
	//log.Infof("after delete ents:%v, stabled:%v", l.entries, l.stabled)
	return nil
}

// get entries after specific index (index included)
func (l *RaftLog) getFollowingEntries(index uint64) []*pb.Entry {
	var res []*pb.Entry
	for ; index-l.first < uint64(len(l.entries)); index++ {
		res = append(res, &l.entries[index-l.first])
	}
	return res
}

func (l *RaftLog) setCommitted(i uint64) {
	l.committed = i
}

func (l *RaftLog) setApplied(i uint64) {
	l.applied = i
}

func (l *RaftLog) setStabled(i uint64) {
	l.stabled = i
}
