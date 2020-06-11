#!/bin/bash

echo $(date) wrap-shifter: just inside shifter
cd $1
shift

export DM_ROOT=/opt/lsst/software/stack
source $DM_ROOT/loadLSST.bash
setup lsst_distrib
setup obs_lsst

export OMP_NUM_THREADS=1

bash $*
R=$?
echo $(date) wrap-shifter: executable finished with return code $R
exit $R
