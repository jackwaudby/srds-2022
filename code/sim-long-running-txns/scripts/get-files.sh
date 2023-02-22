#!/bin/bash

for p in single multi; do
    for a in $(seq 10 10 100); do
        sudo docker cp sim-infinite-queue-epoch-"$p"-"$a":/results.csv ../data/sim-infinite-queue-epoch-"$p"-"$a".csv
    done
done
