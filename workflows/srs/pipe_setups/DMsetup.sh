#!/bin/bash

# Workaround for EUPS trying to write to home directory
if [[ $SITE == "NERSC" ]]
then
  export HOME=/tmp/${USER}/${PIPELINE_PROCESSINSTANCE}
  mkdir -p $HOME
  (cd $HOME && tar -xzf ${SCRIPT_LOCATION}/home.tgz)
else
  export HOME=`pwd`
fi

# Setup for the stack
source ${DM_SETUP}
setup lsst_distrib

if [[ $SITE=="NERSC" ]]
then
  setup obs_lsstSim w.2018.04-32-g6deb1ad -t w_2018_09
fi

if [[ $DM_SETUP == *"/sps/lsst/software/lsst_distrib/"* ]]
then
    cd /sps/lsst/users/nchotard/obs_cfht
    eups declare -r . obs_cfht tractbugfix
    setup obs_cfht tractbugfix
    cd -
fi
