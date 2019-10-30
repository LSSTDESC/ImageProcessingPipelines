#!/bin/bash -x

cd $1
shift

source /opt/lsst/software/stack/loadLSST.bash
setup lsst_distrib
setup obs_lsst
export PYTHONPATH=/global/homes/b/bxc/dm/parsl:$PYTHONPATH
export PATH=/global/homes/b/bxc/dm/parsl/parsl/executors/high_throughput/:$PATH

export export OMP_NUM_THREADS=1

bash $*
