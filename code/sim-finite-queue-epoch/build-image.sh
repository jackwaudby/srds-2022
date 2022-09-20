#!/bin/bash

mvn clean install

docker build -t sim-finite-queue-epoch:latest .
