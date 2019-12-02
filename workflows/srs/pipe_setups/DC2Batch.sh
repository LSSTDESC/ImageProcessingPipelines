#!/bin/bash

# IN2P3 would prefer to run without exit on error
# see: https://github.com/LSSTDESC/ImageProcessingPipelines/issues/19
#if [ $SITE == "NERSC" ]
#then
#  set -e # exit on error
#fi

# Get the local configuration
source ${LOCAL_CONFIG}

if [[ $SITE == "LSST-IN2P3" ]]
then
  # Setup DM stack
  source ${SETUP_LOCATION}/DMsetup.sh
fi

ulimit -c ${CORE_LIMIT:-1000} # Limit core dump

# Set up a unique work directory for this pipeline stream
stream=$(echo $PIPELINE_STREAMPATH | cut -f1 -d.)
export WORK_DIR=${OUTPUT_DATA_DIR}/work/${stream}

# Only set IN_DIR and OUT_DIR if not already set
export DATA_DIR=${OUT_DIR:-${OUTPUT_DATA_DIR}/data}
export OUT_DIR=${OUT_DIR:-${OUTPUT_DATA_DIR}/data/output}
export IN_DIR=${IN_DIR:-${OUTPUT_DATA_DIR}/data/input}

# Setup reprocessing scripts
export PATH=$PATH:$SCRIPT_LOCATION

export OMP_NUM_THREADS=1

source ${CHECKPOINTS}


# Launch the setup/script
export SCRIPT=${SETUP_LOCATION}/${PIPELINE_PROCESS:-$1}
if [ $SITE == "NERSC" ] # NERSC when SHIFTER is not used
then
  source ${SETUP_LOCATION}/DMsetup.sh; set -xe; export SHELLOPTS; source ${SCRIPT}
else # IN2P3
  # set -xe; export SHELLOPTS;
  source ${SCRIPT}
fi
