#/bin/bash

mvn clean install

sudo docker build -t sim-infinite-queue-epoch:latest .
