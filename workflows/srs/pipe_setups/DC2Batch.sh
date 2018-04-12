#!/bin/bash

# IN2P3 would prefer to run without exit on error
# see: https://github.com/LSSTDESC/ImageProcessingPipelines/issues/19
if [ $SITE == "NERSC" ]
then
  set -e # exit on error
fi

# Get the local configuration
source ${LOCAL_CONFIG}

if [[ $SITE == "LSST-IN2P3" ]]
then
  # Setup DM stack
  source ${SETUP_LOCATION}/DMsetup.sh
fi

ulimit -c ${CORE_LIMIT:-1000} # Limit core dump
# set -e # exit on error

# Set up a unique work directory for this pipeline stream
stream=$(echo $PIPELINE_STREAMPATH | cut -f1 -d.)
export WORK_DIR=${OUTPUT_DATA_DIR}/work/${stream}

# Only set IN_DIR and OUT_DIR if not already set
export OUT_DIR=${OUT_DIR:-${WORK_DIR}/output}
export IN_DIR=${IN_DIR:-${WORK_DIR}/input}

# Setup reprocessing scripts
export PATH=$PATH:$SCRIPT_LOCATION

# Launch the setup/script
export SCRIPT=${SETUP_LOCATION}/${PIPELINE_PROCESS:-$1}

# For now, scripts that use pipelineSet need to be handled outside Shifter
if [ $SITE == "NERSC" ] && (echo $PIPELINE_PROCESS | grep "setup_");
then
  export OMP_NUM_THREADS=1
  source ${SETUP_LOCATION}/DMsetup.sh; set -xe; export SHELLOPTS; source ${SCRIPT}
elif [ -v SHIFTER_IMAGE ] # Use Shifter if available
then
export OMP_NUM_THREADS=1
/usr/bin/time -v shifter --image=${SHIFTER_IMAGE} /bin/bash <<EOF
echo "Running shifter image ${SHIFTER_IMAGE}"
export OMP_NUM_THREADS=1
export PATH=$PATH:$SCRIPT_LOCATION
source /opt/lsst/software/stack/loadLSST.bash
setup lsst_distrib -t current 
setup obs_lsstSim -t dc2
set -xe; export SHELLOPTS; source ${SCRIPT}
EOF
elif [ $SITE == "NERSC" ] # NERSC when SHIFTER is not used
then
  export OMP_NUM_THREADS=1
  source ${SETUP_LOCATION}/DMsetup.sh; set -xe; export SHELLOPTS; source ${SCRIPT}
else # IN2P3
  # set -xe; export SHELLOPTS;
  source ${SCRIPT}
fi
