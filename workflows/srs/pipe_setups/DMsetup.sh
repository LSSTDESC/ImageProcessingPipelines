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
echo "source ${DM_SETUP}"
source ${DM_SETUP}
setup lsst_distrib


# A specific version of obs_lsstCam is needed for now
if [[ $SITE == "NERSC" ]]
then
    setup obs_lsstSim w.2018.04-35-g11f44ee -t w_2018_09
else
    eups undeclare obs_lsstCam localver
    eups declare --force -r $ROOT_SOFTS/obs_lsstCam obs_lsstCam localver
    setup obs_lsstCam localver
fi
