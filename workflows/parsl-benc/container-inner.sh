#!/bin/bash

cd $1
shift

source /opt/lsst/software/stack/loadLSST.bash
setup lsst_distrib
setup obs_lsst

export export OMP_NUM_THREADS=1

bash $*
