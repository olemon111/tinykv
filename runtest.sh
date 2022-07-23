#!/bin/bash

#for i in {1..20}; do make project2b >> ./testlog/test2b.log 2>&1; done
#for i in {1..40}; do make project2c >> ./testlog/test2c.log 2>&1; done

for i in {1..30}; do GO111MODULE=on go test >> ./testlog/2c.log 2>&1 -v --count=1 --parallel=1 -p=1 ./kv/test_raftstore -run ^TestSnapshotUnreliableRecoverConcurrentPartition2C|| true; done
