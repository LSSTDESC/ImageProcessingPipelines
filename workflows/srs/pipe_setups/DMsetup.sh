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

#utility functions
verify_checkpoint () {
    var=`grep $1 ${CHECKPOINTS}`
    #check that process is not commented
    if [ "${var%${var#?}}"x != '#x' ]
    then
        local status=`echo ${var: -3:1}`
    else
        local status=""
    fi
    echo $status
} 



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

    echo "setting up local libraries"
    echo "-- checking pyarrow..."
    export PYTHONPATH=$PYTHONPATH:${ROOT_SOFTS}/pyarrow_conda/lib/python3.7/site-packages
    python -c "import pyarrow;print(f'pyarrow:{pyarrow.__version__}')"
    export PYTHONPATH=${ROOT_SOFTS}/local/lib/python3.7/site-packages:$PYTHONPATH
    echo "-- checking ngmix..."
    python -c "import ngmix;print(\"NGMIX %s\"%ngmix.__version__)"
    echo "-- checking GCR..."
    python -c "import GCR;print(f'GCR:{GCR.__version__}')"
    echo "-- checking GCRtalogs..."
    python -c "import GCRCatalogs;print(f'GCRCatalogs:{GCRCatalogs.__version__}')"
    
    echo "setting obs_lsst"
    eups undeclare obs_lsst dc2-run2.1
    eups declare -r $ROOT_SOFTS/obs_lsst obs_lsst dc2-run2.1
    setup obs_lsst  dc2-run2.1
    eups list obs_lsst
    cd ${ROOT_SOFTS}/obs_lsst;git status;cd -

    echo "setting meas_extensions_ngmix"
    eups undeclare meas_extensions_ngmix  dc2-run2.1
    eups declare -r $ROOT_SOFTS/meas_extensions_ngmix meas_extensions_ngmix dc2-run2.1
    setup meas_extensions_ngmix  dc2-run2.1
    eups list meas_extensions_ngmix
    setup meas_extensions_psfex
    cd ${ROOT_SOFTS}/meas_extensions_ngmix;git status;cd -

    # pipe_analysis not ready for parallel execution
    # echo "setting pipe_analysis for QA"
    # eups undeclare pipe_analysis dc2-run2.1
    # eups declare -r $ROOT_SOFTS/pipe_analysis pipe_analysis dc2-run2.1
    # setup display_matplotlib
    # setup meas_extensions_astrometryNet
    # setup pipe_analysis  dc2-run2.1
    # eups list pipe_analysis
fi

