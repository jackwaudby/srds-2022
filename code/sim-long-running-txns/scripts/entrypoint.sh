#!/bin/sh
java -cp sim.jar Main -n 64 -b 1.7 -c 1.7 -mu 1 -xi 675000 -eta 1800000 -m 10 -af true -a "$1" -p "$2" -d "$3"
