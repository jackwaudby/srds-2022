#!/bin/bash

for p in single multi; do
    for a in $(seq 10 10 100); do
        docker cp sim-finite-queue-epoch-"$p"-"$a"-"$1":/results.csv ../data/sim-finite-queue-epoch-"$p"-"$a"-"$1".csv
    done
done
