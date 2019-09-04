#!/bin/bash -ex

shifter --image=lsstdesc/stack-sims:w_2019_19-sims_w_2019_19 bash -c "source /opt/lsst/software/stack/loadLSST.bash ; setup lsst_distrib ; setup obs_lsst ; /global/homes/b/bxc/.local/cori/3.7-anaconda-2019.07/bin/process_worker_pool.py $*"

