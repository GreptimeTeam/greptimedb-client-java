#!/bin/bash
protoc -I=proto  --descriptor_set_out=proto/greptimedb.desc --java_out=../java/ proto/*.proto
