#!/bin/bash

for m in 0.0 0.01 0.02 0.03 0.04 0.05 0.06 0.07 0.08 0.09 0.1
do
    java -cp ../target/sim-decentralised-1.0-SNAPSHOT.jar Main --duration 1 -pd 0.0 -pl "$m"
done