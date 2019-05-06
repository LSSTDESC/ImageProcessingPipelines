#!/bin/bash

# Workaround for EUPS trying to write to home directory
#if [[ $SITE == "NERSC" ]]
#then
#  export HOME=/tmp/${USER}/${PIPELINE_PROCESSINSTANCE}
#  mkdir -p $HOME
#  (cd $HOME && tar -xzf ${SETUP_LOCATION}/home.tgz)
#else
export HOME=`pwd`
#fi

# Setup for the stack
source ${DM_SETUP}
setup lsst_distrib


if [[ $SITE == "NERSC" ]]
then
    setup -r ${ROOT_SOFTS}/obs_lsst -j
else
    # eups undeclare obs_lsst dc2-run1.2-v3
    # eups declare -r $ROOT_SOFTS/obs_lsst obs_lsst dc2-run1.2-v3
    # eups declare -r ${ROOT_SOFTS}/pipe_tasks pipe_tasks u-rearmstr-desc-ccd-fix_w39
    # setup obs_lsst  dc2-run1.2-v3
    # setup pipe_tasks u-rearmstr-desc-ccd-fix_w39

    eups declare -r $ROOT_SOFTS/obs_lsst obs_lsst dc2-run2.1
    setup obs_lsst  dc2-run2.1
    eups declare -r $ROOT_SOFTS/meas_extensions_ngmix meas_extensions_ngmix dc2-run2.1
    setup meas_extensions_ngmix  dc2-run2.1

    # setup obs_lsst
    # eups declare -r ${ROOT_SOFTS}/meas_extensions_ngmix meas_extensions_ngmix local_w13
    # setup meas_extensions_ngmix local_w13

    export PYTHONPATH=${ROOT_SOFTS}/ngmix/build/lib:$PYTHONPATH
    eups list obs_lsst
    eups list meas_extensions_ngmix
    python -c "import ngmix"
fi
eups list obs_lsst
