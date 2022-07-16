#!/bin/bash

for i in {1..50}; do make project2b >> ./testlog/test2b.log 2>&1; done
