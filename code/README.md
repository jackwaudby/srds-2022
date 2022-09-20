# Epoch-based Commit Simulations

There are 3 simulation programs:
1. `sim-infinite-queue-epoch`:
    * Nodes are never idle
    * Failures are detected at epoch-level granularity 
    * Lost jobs have same service time as completed jobs
2. `sim-finite-queue-epoch`:
    * There is an external queue, nodes can be idle
    * Failures are detected at epoch-level granularity 
    * Lost jobs have same service time as completed jobs and are retried infinitely 
3. `sim-commit-groups`: simulate non-failing epochs running TPC-C workload. Used to determine the number of commit groups. 

## Parameters

Default parameters, the unit is `ms` unless specified otherwise:
* Cluster size: `n=50`
* Epoch timeout: `a=100`
* Commit operation service rate: `b=10`
* Abort operation service rate: `c=10`
* Transaction service rate: `mu=1`
* Mean time between failures: `phi=20000`
* Repair rate: `rho=1000`
* Fixed seed: `s=false`
* Time limit: `d=3600` (seconds)
* Protocol: `p=single`; options are `multi` and `single`
* Proportion of distributed transaction: `m=1`
* Affinity: `af=false`

`sim-finite-queue-epoch` only:
* Set seed value: `sv=0`
* Arrival rate: `ar=0.0208`

Results are saved to `simulation/results.csv`, the following statistics are reported:
* Completed jobs per ms 
* Lost jobs per ms 
* Average number of commit groups (multi-commit only)
* Average response time (`sim-finite-queue-epoch` only)

## Run

Each simulation program is dockerised and is run for each protocol across a range of `a` values.

```
# build docker image with latest jar
cd <sim>
./build-image.sh

# run docker containers
cd scripts
./coordinator.sh <time>

# extract results
./get-files.sh

# generate plots 
./make-plots.sh
```