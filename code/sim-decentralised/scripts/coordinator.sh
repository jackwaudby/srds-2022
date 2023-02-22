#!/bin/bash

yes | sudo docker container prune

for p in single multi; do
    for a in $(seq 10 10 100); do
             sudo docker run -d --name sim-infinite-queue-epoch-"$p-$a" sim-infinite-queue-epoch:latest "$a" "$p" $1
    done
done
