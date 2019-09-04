#!/bin/bash -ex

shifter --image=lsstdesc/stack-sims:w_2019_19-sims_w_2019_19 bash -c "source /opt/lsst/software/stack/loadLSST.bash ; setup lsst_distrib ; setup obs_lsst ; export PYTHONPATH=/global/homes/b/bxc/dm/parsl:$PYTHONPATH; /global/homes/b/bxc/dm/parsl/parsl/executors/high_throughput/process_worker_pool.py $*"

