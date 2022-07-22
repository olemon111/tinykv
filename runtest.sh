#!/bin/bash

for i in {1..20}; do make project2b >> ./testlog/test2b.log 2>&1; done
for i in {1..40}; do make project2c >> ./testlog/test2c.log 2>&1; done
