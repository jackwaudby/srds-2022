#!/bin/bash

yes | docker container prune

if [ "$2" == "fixed" ]
then
    FIXED="true"
else
    FIXED="false"
fi

for p in single multi; do
    for a in $(seq 10 10 100); do
        docker run -d --name "sim-finite-queue-epoch-$p-$a-$2" sim-finite-queue-epoch:latest "$a" "$p" "$1" "$FIXED"
    done
done
