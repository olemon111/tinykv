#!/bin/bash

#for i in {1..20}; do make project2b >> ./testlog/test2b.log 2>&1; done
#for i in {1..50}; do make project2c >> ./testlog/test2c.log 2>&1; done
#for i in {1..100}; do GO111MODULE=on go test >> ./testlog/testfirst.log 2>&1 -v --count=1 --parallel=1 -p=1 ./kv/test_raftstore -run ^TestOneSplit3B|| true; done
for i in {1..10}; do make project3b >> ./testlog/test3b.log 2>&1; done
for i in {1..20}; do make project2 >> ./testlog/test2.log 2>&1; done

