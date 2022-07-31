package raftstore

import (
	"fmt"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"reflect"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) applyEntry(entry *pb.Entry, kvWB *engine_util.WriteBatch) *engine_util.WriteBatch {
	switch entry.EntryType {
	case pb.EntryType_EntryNormal:
		kvWB = d.applyNormalEntry(entry, kvWB)
	case pb.EntryType_EntryConfChange:
		d.applyConfChangeEntry(entry, kvWB)
	}
	return kvWB
}

func (d *peerMsgHandler) applyConfChangeEntry(entry *pb.Entry, kvWB *engine_util.WriteBatch) {
	// unmarshal confChange
	cc := pb.ConfChange{}
	err := cc.Unmarshal(entry.Data)
	log.Infof("%v apply conf change, cc:%v", d.Tag, cc)
	if err != nil {
		log.Panicf("unmarshal confchange error %v", err)
		return
	}
	// unmarshal request
	var msg raft_cmdpb.RaftCmdRequest
	err = msg.Unmarshal(cc.Context)
	if err != nil {
		log.Panicf("unmarshal confChange context error %v", err)
		return
	}
	// check regionEpoch
	err = util.CheckRegionEpoch(&msg, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok { // FIXME: seems unnecessary
		log.Infof("%v confchange region epoch not match err:%v", d.Tag, errEpochNotMatching)
		d.matchProposal(ErrResp(err), entry, nil)
		return
	}
	region := d.Region()
	if cc.ChangeType == pb.ConfChangeType_AddNode {
		log.Infof("%v apply add node %v", d.Tag, cc)
		if !d.checkNodeInRegion(cc.NodeId, region) {
			// update regionLocalState
			region.RegionEpoch.ConfVer++
			newPeer := msg.AdminRequest.ChangePeer.Peer
			region.Peers = append(region.Peers, newPeer)
			meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
			// FIXME: it seems no need to update region state in global context for test case needs empty
			sm := d.ctx.storeMeta
			sm.Lock()
			sm.regions[region.Id] = region // maybe no use?
			sm.Unlock()
			// update peer cache
			d.insertPeerCache(newPeer)
			log.Infof("%v insert peer cache %v", d.Tag, newPeer)
		}
	} else if cc.ChangeType == pb.ConfChangeType_RemoveNode {
		log.Infof("%v apply remove node %v", d.Tag, cc)
		//log.Infof("%v before update regionid:%v, nodeid:%v", d.Tag, d.regionId, cc.NodeId)
		if cc.NodeId == d.PeerId() { // remove self
			log.Infof("%v remove self %v", d.Tag, cc.NodeId)
			// FIXME: not sure if necessary
			region := d.Region()
			if d.IsLeader() && len(region.Peers) == 2 {
				peer := raft.None
				for _, p := range region.Peers {
					if p.Id != d.PeerId() {
						peer = p.Id
						break
					}
				}
				if peer != raft.None {
					msg := []pb.Message{
						{
							To:      peer,
							MsgType: pb.MessageType_MsgHeartbeat,
							Commit:  d.peerStorage.raftState.HardState.Commit,
						},
					}
					for i := 0; i < 10; i++ {
						d.Send(d.ctx.trans, msg)
						time.Sleep(100 * time.Millisecond)
					}
				}
			}
			d.destroyPeer()
			return // FIXME: maybe other actions after destroy self?
		}
		if d.checkNodeInRegion(cc.NodeId, region) {
			log.Infof("%v not in region:%v, drop", cc.NodeId, region)
			// update regionLocalState
			region.RegionEpoch.ConfVer++
			for i, p := range region.Peers { // remove peer
				if p.Id == cc.NodeId {
					region.Peers = append(region.Peers[:i], region.Peers[i+1:]...)
					break
				}
			}
			meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
			// FIXME: it seems no need to update region state in global context for test case needs empty
			sm := d.ctx.storeMeta
			sm.Lock()
			sm.regions[region.Id] = region // maybe no use?
			sm.Unlock()
			// update peer cache
			d.removePeerCache(cc.NodeId)
		}
	}
	// propose by raft
	d.RaftGroup.ApplyConfChange(cc)
	// response
	resp := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
			ChangePeer: &raft_cmdpb.ChangePeerResponse{
				//Region: d.Region(),
			},
		},
	}
	d.matchProposal(resp, entry, nil)
	if d.IsLeader() {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender) // FIXME: not sure of usage
	}
	//d.notifyHeartbeatScheduler(d.Region(), d.peer)
}

func (d *peerMsgHandler) notifyHeartbeatScheduler(region *metapb.Region, peer *peer) {
	clonedRegion := new(metapb.Region)
	err := util.CloneMsg(region, clonedRegion)
	if err != nil {
		return
	}
	d.ctx.schedulerTaskSender <- &runner.SchedulerRegionHeartbeatTask{
		Region:          clonedRegion,
		Peer:            peer.Meta,
		PendingPeers:    peer.CollectPendingPeers(),
		ApproximateSize: peer.ApproximateSize,
	}
}

func (d *peerMsgHandler) applyNormalEntry(entry *pb.Entry, kvWB *engine_util.WriteBatch) *engine_util.WriteBatch {
	// unmarshal request
	msg := &raft_cmdpb.RaftCmdRequest{}
	err := msg.Unmarshal(entry.Data)
	if err != nil {
		log.Panicf("unmarshal raft command request error %v", err)
		return kvWB
	}
	//log.Infof("apply normal entry %v", msg)
	// handle requests
	if msg.AdminRequest != nil {
		kvWB = d.applyAdminRequest(msg, entry, kvWB)
	} else if len(msg.Requests) > 0 {
		kvWB = d.applyNormalRequest(msg, entry, kvWB)
	}
	return kvWB
}

func (d *peerMsgHandler) applyAdminRequest(msg *raft_cmdpb.RaftCmdRequest, entry *pb.Entry, kvWB *engine_util.WriteBatch) *engine_util.WriteBatch {
	req := msg.AdminRequest
	resp := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
	}
	switch req.GetCmdType() {
	case raft_cmdpb.AdminCmdType_InvalidAdmin:
		log.Panicf("apply invalid admin request")
	case raft_cmdpb.AdminCmdType_ChangePeer: // apply in confChange
	case raft_cmdpb.AdminCmdType_CompactLog:
		kvWB = d.applyCompactLog(req, kvWB)
	case raft_cmdpb.AdminCmdType_TransferLeader: // will not happen
	case raft_cmdpb.AdminCmdType_Split:
		err := d.applySplit(msg, resp, kvWB)
		if err != nil {
			d.matchProposal(ErrResp(err), entry, nil)
			return kvWB
		}
		kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
		kvWB = &engine_util.WriteBatch{}
		//if d.IsLeader() {
		//	d.HeartbeatScheduler(d.ctx.schedulerTaskSender) // FIXME: not sure
		//}
	}
	return kvWB
}

func (d *peerMsgHandler) applyCompactLog(req *raft_cmdpb.AdminRequest, kvWB *engine_util.WriteBatch) *engine_util.WriteBatch {
	if req.CompactLog.CompactIndex >= d.peerStorage.applyState.TruncatedState.Index && req.CompactLog.CompactIndex <= d.peerStorage.AppliedIndex() { // in case of no need to gc
		//prev := &rspb.RaftApplyState{
		//	AppliedIndex: d.peerStorage.applyState.AppliedIndex,
		//	TruncatedState: &rspb.RaftTruncatedState{
		//		Index: d.peerStorage.applyState.TruncatedState.Index,
		//		Term:  d.peerStorage.applyState.TruncatedState.Term,
		//	},
		//}
		// update raftApplyState
		d.peerStorage.applyState.TruncatedState.Term = req.CompactLog.GetCompactTerm()
		d.peerStorage.applyState.TruncatedState.Index = req.CompactLog.GetCompactIndex()
		_ = kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
		//log.Infof("%s apply admin compact log, req:%v, applystate: %v to %v", d.Tag, req, prev, d.peerStorage.applyState)
		kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
		kvWB = &engine_util.WriteBatch{}
		// schedule raftLogGCTask to do log deletion work asynchronously
		d.ScheduleCompactLog(d.peerStorage.applyState.TruncatedState.Index)
	}
	return kvWB
}

func (d *peerMsgHandler) applySplit(msg *raft_cmdpb.RaftCmdRequest, resp *raft_cmdpb.RaftCmdResponse, kvWB *engine_util.WriteBatch) error {
	// check region
	if msg.Header.RegionId != d.regionId {
		log.Infof("%v region id not match %v != %v", d.Tag, msg.Header.RegionId, d.regionId)
		return &util.ErrRegionNotFound{RegionId: msg.Header.RegionId}
	}
	// check regionEpoch
	err := util.CheckRegionEpoch(msg, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		log.Infof("%v region epoch not match, err:%v", d.Tag, errEpochNotMatching)
		return errEpochNotMatching
	}
	// check key in region
	split := msg.AdminRequest.Split
	err = util.CheckKeyInRegion(split.SplitKey, d.Region())
	if err != nil {
		log.Infof("%v key not in region %v", d.Tag, err)
		return err
	}
	// check new peer count
	if len(split.NewPeerIds) != len(d.Region().Peers) {
		log.Infof("%v apply split err, new peer count not match %v != %v", d.Tag, len(msg.AdminRequest.Split.NewPeerIds), len(d.Region().Peers))
		return &util.ErrStaleCommand{}
	}
	// apply split
	// update global region state
	sm := d.ctx.storeMeta
	sm.Lock()
	oldRegion := d.Region()
	newRegion := &metapb.Region{
		Id:       split.NewRegionId,
		StartKey: split.SplitKey,
		EndKey:   oldRegion.EndKey,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: InitEpochConfVer,
			Version: InitEpochVer,
		},
	}
	sm.regionRanges.Delete(&regionItem{region: oldRegion})
	// update old region
	oldRegion.RegionEpoch.Version++
	log.Infof("%v apply split, start:%s, end:%s, key:%s", d.Tag, d.Region().StartKey, d.Region().EndKey, split.SplitKey)
	// old: startKey -> splitKey, new: splitKey -> endKey
	oldRegion.EndKey = split.SplitKey
	for i, id := range split.NewPeerIds {
		peer := &metapb.Peer{
			Id:      id,
			StoreId: oldRegion.Peers[i].StoreId,
		}
		newRegion.Peers = append(newRegion.Peers, peer)
		d.insertPeerCache(peer) // FIXME: not sure if necessary
	}
	newPeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
	if err != nil {
		log.Infof("create peer error: %v", err)
		return err
	}
	log.Infof("split, old:%v, new:%v", oldRegion, newRegion)
	// FIXME: choose between
	//sm.setRegion(oldRegion, d.peer)
	//sm.setRegion(newRegion, newPeer)
	sm.regions[d.regionId] = oldRegion
	sm.regions[split.NewRegionId] = newRegion
	sm.regionRanges.ReplaceOrInsert(&regionItem{region: oldRegion})
	sm.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
	sm.Unlock()
	// register new peer in router
	d.ctx.router.register(newPeer)
	err = d.ctx.router.send(newRegion.Id, message.Msg{ // start
		Type:     message.MsgTypeStart,
		RegionID: newRegion.Id,
		Data:     nil,
	})
	if err != nil {
		log.Infof("router send new region start error: %v", err)
		return err
	}
	// write regionLocalState to kvDB
	meta.WriteRegionState(kvWB, oldRegion, rspb.PeerState_Normal)
	meta.WriteRegionState(kvWB, newRegion, rspb.PeerState_Normal)
	// FIXME: not understand
	d.SizeDiffHint = 0
	d.ApproximateSize = new(uint64)
	*d.ApproximateSize = 0
	if err != nil {
		log.Panicf("write region state err: %v", err)
		return err
	}
	// response
	resp.AdminResponse = &raft_cmdpb.AdminResponse{
		CmdType: raft_cmdpb.AdminCmdType_Split,
		Split: &raft_cmdpb.SplitResponse{
			Regions: []*metapb.Region{oldRegion, newRegion},
		},
	}
	//kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
	//kvWB = &engine_util.WriteBatch{}
	//d.notifyHeartbeatScheduler(oldRegion, d.peer)
	if d.IsLeader() {
		d.notifyHeartbeatScheduler(oldRegion, d.peer)
		d.notifyHeartbeatScheduler(newRegion, newPeer)
	}
	return err
}

func (d *peerMsgHandler) applyNormalRequest(msg *raft_cmdpb.RaftCmdRequest, entry *pb.Entry, kvWB *engine_util.WriteBatch) *engine_util.WriteBatch {
	resp := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
	}
	var txn *badger.Txn
	req := msg.Requests[0]
	//log.Infof("apply normal request: %v", req)
	switch req.GetCmdType() {
	case raft_cmdpb.CmdType_Invalid:
		log.Panicf("invalid raft command")
	case raft_cmdpb.CmdType_Get:
		val, err := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Get.Cf, req.Get.Key)
		log.Infof("%v apply get, key:%s, value:%s, err:%v", d.Tag, req.Get.Key, val, err)
		if err != nil {
			log.Infof("get cf error:%v", err)
			val = nil
		}
		resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
			CmdType: raft_cmdpb.CmdType_Get,
			Get: &raft_cmdpb.GetResponse{
				Value: val,
			},
		})
		//update raftApplyState
		if entry.Index > d.peerStorage.applyState.AppliedIndex {
			//prev := &rspb.RaftApplyState{
			//	AppliedIndex: d.peerStorage.applyState.AppliedIndex,
			//	TruncatedState: &rspb.RaftTruncatedState{
			//		Index: d.peerStorage.applyState.TruncatedState.Index,
			//		Term:  d.peerStorage.applyState.TruncatedState.Term,
			//	},
			//}
			d.peerStorage.applyState.AppliedIndex = entry.Index
			err = kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			//log.Infof("req get, applystate:%v to %v", prev, d.peerStorage.applyState)
			if err != nil {
				log.Panicf("set apply state error:%v", err)
				return kvWB
			}
		}
		//kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
		kvWB = &engine_util.WriteBatch{}
	case raft_cmdpb.CmdType_Put:
		log.Infof("%v apply put key:%s, value:%s", d.Tag, req.Put.Key, req.Put.Value)
		kvWB.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)
		resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
			CmdType: raft_cmdpb.CmdType_Put,
			Put:     &raft_cmdpb.PutResponse{},
		})
		// choose between one
		d.SizeDiffHint += uint64(len(req.Put.Key)) + uint64(len(req.Put.Value)) // FIXME:
		//d.SizeDiffHint += uint64(len(req.Put.Key))
	case raft_cmdpb.CmdType_Delete:
		log.Infof("%v apply delete key:%s", d.Tag, req.Delete.Key)
		kvWB.DeleteCF(req.Delete.Cf, req.Delete.Key)
		resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
			CmdType: raft_cmdpb.CmdType_Delete,
			Delete:  &raft_cmdpb.DeleteResponse{},
		})
		d.SizeDiffHint -= uint64(len(req.Delete.Key)) // FIXME:
	case raft_cmdpb.CmdType_Snap:
		log.Infof("%v apply snap %v", d.Tag, req)
		txn = d.peerStorage.Engines.Kv.NewTransaction(false) // set badger Txn to callback explicitly
		if entry.Term < d.Term() {                           // FIXME:
			log.Infof("reject snap for stale term")
			d.matchProposal(ErrRespStaleCommand(d.Term()), entry, txn)
			return kvWB
		}
		//// check regionEpoch // FIXME: not sure if necessary
		err := util.CheckRegionEpoch(msg, d.Region(), true)
		if err != nil {
			//log.Infof("%v snap region epoch not match, err:%v", d.Tag, err)
			resp = ErrResp(err)
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Snap,
				Snap: &raft_cmdpb.SnapResponse{
					Region: d.Region(),
				},
			})
			// propose callback
			d.matchProposal(resp, entry, txn)
			return kvWB
		}
		//else {
		//log.Infof("%v snap region epoch match, %v", d.Tag, d.Region().RegionEpoch)
		//}
		//if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		//	log.Infof("%v snap region epoch not match, err:%v", d.Tag, errEpochNotMatching)
		//	resp = ErrResp(errEpochNotMatching)
		//	resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
		//		CmdType: raft_cmdpb.CmdType_Snap,
		//		Snap: &raft_cmdpb.SnapResponse{
		//			Region: d.Region(),
		//		},
		//	})
		//	d.matchProposal(resp, entry, txn)
		//	return kvWB
		//}
		resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
			CmdType: raft_cmdpb.CmdType_Snap,
			Snap: &raft_cmdpb.SnapResponse{
				Region: d.Region(),
			},
		})
		// update raftApplyState
		if entry.Index > d.peerStorage.applyState.AppliedIndex {
			//prev := &rspb.RaftApplyState{
			//	AppliedIndex: d.peerStorage.applyState.AppliedIndex,
			//	TruncatedState: &rspb.RaftTruncatedState{
			//		Index: d.peerStorage.applyState.TruncatedState.Index,
			//		Term:  d.peerStorage.applyState.TruncatedState.Term,
			//	},
			//}
			d.peerStorage.applyState.AppliedIndex = entry.Index
			//log.Infof("req snap, applystate:%v to %v", prev, d.peerStorage.applyState)
			err := kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			if err != nil {
				log.Panicf("set meta error")
				return nil
			}
			kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
			kvWB = &engine_util.WriteBatch{}
		}
	}
	// propose callback
	d.matchProposal(resp, entry, txn)
	return kvWB
}

func (d *peerMsgHandler) matchProposal(resp *raft_cmdpb.RaftCmdResponse, entry *pb.Entry, txn *badger.Txn) {
	//log.Infof("match proposals:%v, entry.index:%d, entry.term:%d", d.proposals, entry.Index, entry.Term)
	//for _, p := range d.proposals {
	//	log.Infof("p:%v, term:%v, index:%v, cb:%v", p, p.index, p.term, p.cb)
	//}
	for len(d.proposals) > 0 {
		p := d.proposals[0]
		if p.index == entry.Index {
			if p.term == entry.Term {
				if txn != nil && p.cb != nil { // set for snap, maybe turn into nil later
					//log.Infof("resp:%v, entry:%v, txn:%v, p:%v, cb:%v", resp, entry, txn, p, p.cb)
					p.cb.Txn = txn
				}
				log.Infof("cb.done resp:%v, term:%v, index:%v", resp, p.term, p.index)
				p.cb.Done(resp)
				d.proposals = d.proposals[1:]
				break
			} else {
				log.Infof("cb.done stale, term:%v, index:%v", p.term, p.index)
				NotifyStaleReq(entry.Term, p.cb)
			}
		}
		d.proposals = d.proposals[1:]
	}
}

func (d *peerMsgHandler) checkNodeInRegion(nodeId uint64, region *metapb.Region) bool {
	for _, p := range region.Peers {
		if p.Id == nodeId {
			return true
		}
	}
	return false
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	if !d.RaftGroup.HasReady() {
		return
	}
	ready := d.RaftGroup.Ready()
	// persist state
	applySnapResult, err := d.peerStorage.SaveReadyState(&ready)
	if err != nil {
		log.Infof("save ready state error: %v", err)
	}
	// update region
	if applySnapResult != nil && !reflect.DeepEqual(applySnapResult.Region, d.Region()) {
		d.SetRegion(applySnapResult.Region)
		log.Infof("set new region: %v", d.peerStorage.region)
		// update region state in global context
		sm := d.ctx.storeMeta
		sm.Lock()
		//sm.regions[d.regionId] = applySnapResult.Region // id -> region // FIXME: choose one
		sm.regions[applySnapResult.Region.Id] = applySnapResult.Region // id -> region
		sm.regionRanges.Delete(&regionItem{region: applySnapResult.PrevRegion})
		sm.regionRanges.ReplaceOrInsert(&regionItem{region: applySnapResult.Region})
		sm.Unlock()
	}
	// send raft messages
	//log.Infof("%v send raft msgs:%v", d.Tag, ready.Messages)
	d.Send(d.ctx.trans, ready.Messages)
	// apply committed entries
	kvWB := &engine_util.WriteBatch{}
	// apply entries and update RaftApplyState
	for _, en := range ready.CommittedEntries {
		//log.Infof("apply entry:%v", en)
		kvWB = d.applyEntry(&en, kvWB)
		if d.stopped { // maybe remove self
			// FIXME: maybe more action?
			d.sendDuplicateMessage(d.ctx.trans, ready.Messages)
			return
		}
		if len(ready.CommittedEntries) > 0 && ready.CommittedEntries[len(ready.CommittedEntries)-1].Index > d.peerStorage.applyState.AppliedIndex {
			d.peerStorage.applyState.AppliedIndex = ready.CommittedEntries[len(ready.CommittedEntries)-1].Index
			//log.Infof("%s apply entries, applystate: %v to %v", d.Tag, prev, d.peerStorage.applyState)
		}
		// save applyState to kvDB
		err = kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState) // key: apply_state_key, value: raftApplyState
		if err != nil {
			log.Panicf("set apply state failed: %v", err)
			return
		}
		kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
		kvWB = &engine_util.WriteBatch{}
	}
	// advance raft node
	d.RaftGroup.Advance(ready)
}

func (d *peerMsgHandler) sendDuplicateMessage(trans Transport, msgs []pb.Message) {
	for _, msg := range msgs {
		err := d.sendRaftMessage(msg, trans)
		for err != nil {
			err = d.sendRaftMessage(msg, trans)
		}
	}
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %s", d.Tag, split.SplitKey)
		//log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	if d.stopped {
		log.Panicf("necessary, propose stopped here") // FIXME: temp test
		cb.Done(ErrRespRegionNotFound(d.regionId))
		return
	}
	if msg.AdminRequest != nil {
		d.proposeAdminCommand(msg, cb)
	} else {
		d.proposeNormalCommand(msg, cb)
	}
}

func (d *peerMsgHandler) proposeNormalCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	req := msg.Requests[0]
	log.Infof("%v propose normal command %v, req:%v", d.Tag, msg, req)
	// check key in region
	var key []byte

	switch req.CmdType {
	case raft_cmdpb.CmdType_Get:
		key = req.Get.Key
	case raft_cmdpb.CmdType_Put:
		key = req.Put.Key
	case raft_cmdpb.CmdType_Delete:
		key = req.Delete.Key
	case raft_cmdpb.CmdType_Snap:
	}
	// check key
	if req.CmdType != raft_cmdpb.CmdType_Snap {
		err := util.CheckKeyInRegion(key, d.Region())
		if err != nil {
			log.Infof("cb.done key not in region err:%v", err)
			cb.Done(ErrResp(err))
			return
		}
	}
	// propose
	data, err := msg.Marshal() // marshal request
	if err != nil {
		log.Panicf("marshal raft cmd request error %v", err)
		cb.Done(ErrResp(err))
		return
	}
	// append proposal to callback
	d.appendProposal(cb)
	err = d.RaftGroup.Propose(data)
	if err != nil {
		log.Infof("propose error %v", err)
		cb.Done(ErrResp(err))
		return
	}
}

func (d *peerMsgHandler) appendProposal(cb *message.Callback) {
	d.proposals = append(d.proposals, &proposal{
		index: d.nextProposalIndex(),
		term:  d.Term(),
		cb:    cb,
	})
}

func (d *peerMsgHandler) proposeAdminCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	switch msg.AdminRequest.CmdType {
	case raft_cmdpb.AdminCmdType_InvalidAdmin:
		log.Panicf("invalid admin command")
	case raft_cmdpb.AdminCmdType_ChangePeer:
		d.appendProposal(cb)
		d.proposeChangePeer(msg)
	case raft_cmdpb.AdminCmdType_Split:
		// FIXME: it seems no need to callback
		d.appendProposal(cb)
		err := d.proposeSplit(msg)
		if err != nil {
			log.Infof("cb.done err split %v", err)
			cb.Done(ErrResp(err))
			return
		}
		//log.Infof("%v propose split, start:%s, end:%s key:%s \n msg:%v", d.Tag, d.Region().StartKey, d.Region().EndKey, msg.AdminRequest.Split.SplitKey, msg)
	case raft_cmdpb.AdminCmdType_CompactLog:
		// no need to callback
		d.proposeCompactLog(msg)
	case raft_cmdpb.AdminCmdType_TransferLeader:
		resp := d.proposeTransferLeader(msg)
		log.Infof("cb.done transfer %v", resp)
		cb.Done(resp)
	}
}

func (d *peerMsgHandler) proposeChangePeer(msg *raft_cmdpb.RaftCmdRequest) {
	req := msg.AdminRequest
	ctx, _ := msg.Marshal()
	cc := pb.ConfChange{
		ChangeType: req.ChangePeer.ChangeType,
		NodeId:     req.ChangePeer.Peer.Id,
		Context:    ctx,
	}
	_ = d.RaftGroup.ProposeConfChange(cc)
}

func (d *peerMsgHandler) proposeSplit(msg *raft_cmdpb.RaftCmdRequest) error {
	log.Infof("%v before propose split msg:%v", d.Tag, msg)
	// check region
	if msg.Header.RegionId != d.regionId {
		log.Infof("split key region Id not match %v != %v", msg.Header.RegionId, d.regionId)
		return &util.ErrRegionNotFound{RegionId: msg.Header.RegionId}
	}
	// check key in region
	err := util.CheckKeyInRegion(msg.AdminRequest.Split.SplitKey, d.Region())
	if err != nil {
		log.Infof("split key not in region, err:%v", err)
		return err
	}
	// propose
	data, err := msg.Marshal() // marshal request
	if err != nil {
		log.Panicf("marshal raft cmd request error %v", err)
		return err
	}
	// check new peer count
	if len(msg.AdminRequest.Split.NewPeerIds) != len(d.Region().Peers) {
		log.Infof("%v split new peer count not match %v != %v", d.Tag, len(msg.AdminRequest.Split.NewPeerIds), len(d.Region().Peers))
		return &util.ErrStaleCommand{}
	}
	log.Infof("%v propose split, start:%s, end:%s key:%s \n msg:%v", d.Tag, d.Region().StartKey, d.Region().EndKey, msg.AdminRequest.Split.SplitKey, msg)
	_ = d.RaftGroup.Propose(data)
	return nil
}

func (d *peerMsgHandler) proposeCompactLog(msg *raft_cmdpb.RaftCmdRequest) {
	req := msg.AdminRequest
	// propose
	data, err := msg.Marshal() // marshal request
	if err != nil {
		log.Panicf("marshal raft cmd request error %v", err)
		return
	}
	log.Infof("propose compact log %v", req)
	err = d.RaftGroup.Propose(data)
	if err != nil {
		log.Infof("propose error %v", err)
		return
	}
}

func (d *peerMsgHandler) proposeTransferLeader(msg *raft_cmdpb.RaftCmdRequest) *raft_cmdpb.RaftCmdResponse {
	// transfer directly, no need to replicate in raft group
	d.RaftGroup.TransferLeader(msg.AdminRequest.TransferLeader.Peer.Id)
	return &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
			TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
		},
	}
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	//log.Debugf("%s handle raft message %s from %d to %d",
	//	d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	//from := msg.GetFromPeer()
	to := msg.GetToPeer()
	if msg.Message != nil {
		//log.Debugf("[region %d] handle raft message (type:%s, from:%v, to:%v) from %d to %d", regionID, msg.Message.MsgType, msg.Message.From, msg.Message.To, from.GetId(), to.GetId())
	}
	//log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

/// Checks if the message is sent to the correct peer.
///
/// Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
		//log.Infof("%v raftgclogtick, appliedIdx %v firstIdx %v", d.Tag, appliedIdx, firstIdx)
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
