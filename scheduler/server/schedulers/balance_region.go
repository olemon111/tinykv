// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"sort"

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	// select suitable stores
	var stores []*core.StoreInfo
	for _, s := range cluster.GetStores() {
		if s.IsUp() && s.DownTime() <= cluster.GetMaxStoreDownTime() {
			stores = append(stores, s)
		}
	}
	if len(stores) < 2 { // original and target at least
		return nil
	}
	// sort according to region size, try to choose the store with the biggest region size
	sort.Slice(stores, func(i, j int) bool {
		return stores[i].GetRegionSize() > stores[j].GetRegionSize()
	})
	// select one region to move
	var originalStore *core.StoreInfo
	var region *core.RegionInfo
	for _, s := range stores {
		originalStore = s
		region = cluster.RandPendingRegion(s.GetID())
		if region != nil {
			break
		}
		region = cluster.RandFollowerRegion(s.GetID())
		if region != nil {
			break
		}
		region = cluster.RandLeaderRegion(s.GetID())
		if region != nil {
			break
		}
	}
	if region == nil || len(region.GetPeers()) < cluster.GetMaxReplicas() { // move region only when exceed replica limitation
		return nil
	}
	// select a store as target: the store with the smallest region size
	var targetStore *core.StoreInfo
	for i := range stores {
		s := stores[len(stores)-i-1]
		if _, in := region.GetStoreIds()[s.GetID()]; !in { // target store should not in region
			targetStore = s
			break
		}
	}
	if targetStore == nil { // no target store suitable
		return nil
	}
	// judge whether the operation is valuable
	// the difference between the stores' region size is big enough
	if originalStore.GetRegionSize()-targetStore.GetRegionSize() > 2*region.GetApproximateSize() {
		peer, err := cluster.AllocPeer(targetStore.GetID())
		if err != nil {
			return nil
		}
		movePeerOperator, err := operator.CreateMovePeerOperator("", cluster, region, operator.OpBalance, originalStore.GetID(), targetStore.GetID(), peer.Id)
		if err != nil {
			return nil
		}
		return movePeerOperator
	}
	return nil
}
