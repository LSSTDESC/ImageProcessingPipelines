#!/bin/bash

# Workaround for EUPS trying to write to home directory
if [[ $SITE == "NERSC" ]]
then
  export HOME=/tmp/${USER}/${PIPELINE_PROCESSINSTANCE}
  mkdir -p $HOME
  (cd $HOME && tar -xzf ${SETUP_LOCATION}/home.tgz)
else
  export HOME=`pwd`
fi

# Setup for the stack
source ${DM_SETUP}
setup lsst_distrib


# A specific version of obs_lsstSim is needed for now
if [[ $SITE == "NERSC" ]]
then
    setup obs_lsstSim w.2018.04-35-g11f44ee -t w_2018_09
else
    eups declare -r $ROOT_SOFTS/obs_lsstSim obs_lsstSim localver
    setup obs_lsstSim localver
    eups declare -r $ROOT_SOFTS/pipe_tasks pipe_tasks localver
    setup pipe_tasks localver
fi
