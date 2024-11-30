package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.GetResponse{
		RegionError: nil, // any request might cause a region error
		Error:       nil,
		Value:       nil,
		NotFound:    false,
	}
	reader, err := server.storage.Reader(req.Context)
	// check region error
	if regionError, ok := err.(*raft_storage.RegionError); ok {
		resp.RegionError = regionError.RequestErr
		return resp, nil
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.Version)
	// lock request key
	server.Latches.WaitForLatches([][]byte{req.Key})
	defer server.Latches.ReleaseLatches([][]byte{req.Key})
	// check write conflict
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		resp.Error = &kvrpcpb.KeyError{
			Locked:    nil,
			Retryable: "",
			Abort:     "",
			Conflict:  nil,
		}
		return resp, nil
	}
	if lock != nil && lock.Ts < txn.StartTS { // key locked by others before transaction start
		resp.Error = &kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				PrimaryLock: lock.Primary,
				LockVersion: lock.Ts,
				Key:         req.Key,
				LockTtl:     lock.Ttl,
			},
			Retryable: "",
			Abort:     "",
			Conflict:  nil,
		}
		return resp, nil
	}
	val, err := txn.GetValue(req.Key)
	if err != nil || val == nil { // key not found
		resp.NotFound = true
		return resp, err
	}
	resp.Value = val
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	// check if committed by other transaction in case of write conflict
	resp := &kvrpcpb.PrewriteResponse{
		RegionError: nil,
		Errors:      nil,
	}
	reader, err := server.storage.Reader(req.Context)
	// check region error
	if regionError, ok := err.(*raft_storage.RegionError); ok {
		resp.RegionError = regionError.RequestErr
		return resp, nil
	}
	defer reader.Close()
	// lock request keys
	var keys [][]byte
	for _, m := range req.Mutations {
		keys = append(keys, m.Key)
	}
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)
	// check errors
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	hasError := false
	for _, m := range req.Mutations {
		// check write conflict
		write, commitTs, err := txn.MostRecentWrite(m.Key)
		if err != nil {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Locked:    nil,
				Retryable: "",
				Abort:     "",
				Conflict:  nil,
			})
			hasError = true
			continue
		}
		if write != nil && commitTs > txn.StartTS {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Locked:    nil,
				Retryable: "",
				Abort:     "",
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    txn.StartTS,
					ConflictTs: commitTs,
					Key:        m.Key,
					Primary:    req.PrimaryLock,
				},
			})
			hasError = true
			continue
		}
		// unlock or invisible
		lock, err := txn.GetLock(m.Key)
		if lock != nil && lock.Ts < txn.StartTS {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					Key:         m.Key,
					LockTtl:     lock.Ttl,
				},
				Retryable: "",
				Abort:     "",
				Conflict:  nil,
			})
			hasError = true
			continue
		}
		// write into transaction: default and lock
		switch m.Op {
		case kvrpcpb.Op_Put:
			txn.PutValue(m.Key, m.Value)
			txn.PutLock(m.Key, &mvcc.Lock{
				Primary: req.PrimaryLock,
				Ts:      txn.StartTS,
				Ttl:     req.LockTtl,
				Kind:    mvcc.WriteKindPut,
			})
		case kvrpcpb.Op_Del:
			txn.DeleteValue(m.Key)
			txn.PutLock(m.Key, &mvcc.Lock{
				Primary: req.PrimaryLock,
				Ts:      txn.StartTS,
				Ttl:     req.LockTtl,
				Kind:    mvcc.WriteKindDelete,
			})
		case kvrpcpb.Op_Rollback:
		case kvrpcpb.Op_Lock:
		}
	}
	if hasError {
		return resp, nil
	}
	// write into storage
	return resp, server.storage.Write(req.Context, txn.Writes())
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.CommitResponse{
		RegionError: nil,
		Error:       nil,
	}
	reader, err := server.storage.Reader(req.Context)
	// check region error
	if regionError, ok := err.(*raft_storage.RegionError); ok {
		resp.RegionError = regionError.RequestErr
		return resp, nil
	}
	defer reader.Close()
	// lock request keys
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)
	// check errors
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, key := range req.Keys {
		// check repeat commit
		recentWrite, commitTs, err := txn.MostRecentWrite(key)
		if err != nil {
			return resp, err
		}
		if recentWrite != nil && recentWrite.StartTS == req.StartVersion && commitTs == req.CommitVersion {
			if recentWrite.Kind == mvcc.WriteKindRollback {
				resp.Error = &kvrpcpb.KeyError{
					Locked: nil,
					Abort:  "abort",
				}
			}
			return resp, nil
		}
		lock, err := txn.GetLock(key)
		// lock maybe rollback by others for timeout
		if err != nil || lock == nil {
			return resp, nil
		}
		// other transaction may add lock after rollback
		if lock.Ts != txn.StartTS {
			resp.Error = &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					Key:         key,
					LockTtl:     lock.Ttl,
				},
				Retryable: "retry", // since commit, client must retry util success
				Abort:     "",
				Conflict:  nil,
			}
			return resp, nil
		}
		// write into transaction: write
		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			StartTS: txn.StartTS,
			Kind:    lock.Kind,
		})
		// remove lock
		txn.DeleteLock(key)
	}
	// write into storage
	return resp, server.storage.Write(req.Context, txn.Writes())
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ScanResponse{
		RegionError: nil,
		Pairs:       nil,
	}
	reader, err := server.storage.Reader(req.Context)
	// check region error
	if regionError, ok := err.(*raft_storage.RegionError); ok {
		resp.RegionError = regionError.RequestErr
		return resp, nil
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, txn)
	defer scanner.Close()
	// scan kvPairs
	var kvPairs []*kvrpcpb.KvPair
	for uint32(len(kvPairs)) < req.Limit { // single key error will not cause the whole scan to stop
		key, val, err := scanner.Next()
		if key == nil || err != nil { // scanner exhausted or other error occurs
			break
		}
		pair := &kvrpcpb.KvPair{
			Key:   key,
			Value: val,
		}
		lock, err := txn.GetLock(key)
		if err != nil {
			break
		}
		if lock != nil && lock.Ts < txn.StartTS {
			pair.Error = &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					Key:         key,
					LockTtl:     lock.Ttl,
				},
			}
		}
		if val != nil {
			kvPairs = append(kvPairs, pair)
		}
	}
	resp.Pairs = kvPairs
	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.CheckTxnStatusResponse{
		RegionError:   nil,
		LockTtl:       0,
		CommitVersion: 0,
		Action:        kvrpcpb.Action_NoAction,
	}
	reader, err := server.storage.Reader(req.Context)
	// check region error
	if regionError, ok := err.(*raft_storage.RegionError); ok {
		resp.RegionError = regionError.RequestErr
		return resp, nil
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.LockTs)
	// check primary key for committed or rollback
	write, commitTs, err := txn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		return resp, err
	}
	if write != nil && write.Kind != mvcc.WriteKindRollback { // already committed
		resp.CommitVersion = commitTs
		return resp, nil
	}
	// check lock for committed or rollback
	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		resp.Action = kvrpcpb.Action_LockNotExistRollback
		return resp, err
	}
	if lock == nil { // already rollback
		txn.PutWrite(req.PrimaryKey, txn.StartTS, &mvcc.Write{
			StartTS: txn.StartTS,
			Kind:    mvcc.WriteKindRollback,
		})
		resp.Action = kvrpcpb.Action_LockNotExistRollback
		return resp, server.storage.Write(req.Context, txn.Writes())
	}
	// return the status of the lock
	resp.LockTtl = lock.Ttl
	if mvcc.PhysicalTime(req.CurrentTs) >= mvcc.PhysicalTime(lock.Ts)+lock.Ttl { // already timeout
		txn.DeleteLock(req.PrimaryKey) // removes expired locks
		txn.DeleteValue(req.PrimaryKey)
		txn.PutWrite(req.PrimaryKey, txn.StartTS, &mvcc.Write{
			StartTS: txn.StartTS,
			Kind:    mvcc.WriteKindRollback,
		})
		resp.Action = kvrpcpb.Action_TTLExpireRollback
		return resp, server.storage.Write(req.Context, txn.Writes())
	}
	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.BatchRollbackResponse{
		RegionError: nil,
		Error:       nil,
	}
	reader, err := server.storage.Reader(req.Context)
	// check region error
	if regionError, ok := err.(*raft_storage.RegionError); ok {
		resp.RegionError = regionError.RequestErr
		return resp, nil
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)
	// handle request keys
	for _, key := range req.Keys {
		// check current write
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return resp, err
		}
		if write != nil {
			if write.Kind == mvcc.WriteKindRollback { // already rollback
				continue // skip
			} else {
				resp.Error = &kvrpcpb.KeyError{
					Locked:    nil,
					Retryable: "",
					Abort:     "abort",
					Conflict:  nil,
				}
				return resp, nil
			}
		}
		// check lock
		lock, err := txn.GetLock(key)
		if err != nil {
			return resp, err
		}
		txn.PutWrite(key, txn.StartTS, &mvcc.Write{
			StartTS: txn.StartTS,
			Kind:    mvcc.WriteKindRollback,
		})
		if lock != nil && lock.Ts != txn.StartTS { // lock conflict with others
			return resp, server.storage.Write(req.Context, txn.Writes())
		}
		if lock != nil && lock.Ts == txn.StartTS {
			txn.DeleteLock(key)
			txn.DeleteValue(key)
		}
	}
	return resp, server.storage.Write(req.Context, txn.Writes())
}

// KvResolveLock inspects a batch of locked keys and either rolls them all back or commits them all
func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ResolveLockResponse{
		RegionError: nil,
		Error:       nil,
	}
	reader, err := server.storage.Reader(req.Context)
	// check region error
	if regionError, ok := err.(*raft_storage.RegionError); ok {
		resp.RegionError = regionError.RequestErr
		return resp, nil
	}
	defer reader.Close()
	// check locks
	var keys [][]byte
	iter := reader.IterCF(engine_util.CfLock)
	defer iter.Close()
	for ; iter.Valid(); iter.Next() {
		val, err := iter.Item().Value()
		if err != nil {
			return resp, err
		}
		lock, err := mvcc.ParseLock(val)
		if err != nil {
			return resp, err
		}
		if lock.Ts == req.StartVersion {
			keys = append(keys, iter.Item().Key())
		}
	}
	if len(keys) == 0 {
		return resp, nil
	}
	if req.CommitVersion != 0 { // commit
		commitResp, err := server.KvCommit(nil, &kvrpcpb.CommitRequest{
			Context:       req.Context,
			StartVersion:  req.StartVersion,
			Keys:          keys,
			CommitVersion: req.CommitVersion,
		})
		if commitResp.Error != nil || commitResp.RegionError != nil || err != nil {
			return resp, err
		}
	} else { // rollback
		rollbackResp, err := server.KvBatchRollback(nil, &kvrpcpb.BatchRollbackRequest{
			Context:      req.Context,
			StartVersion: req.StartVersion,
			Keys:         keys,
		})
		if rollbackResp.Error != nil || rollbackResp.RegionError != nil || err != nil {
			return resp, err
		}
	}
	return resp, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
