#!/bin/bash

# Get the local configuration
source ${LOCAL_CONFIG}

# Setup DM stack
source ${IN2P3_SCRIPT_LOCATION}/DMsetup.sh

ulimit -c ${CORE_LIMIT:-1000} # Limit core dump
# set -e # exit on error

# Set up a unique work directory for this pipeline stream
stream=$(echo $PIPELINE_STREAMPATH | cut -f1 -d.)
export WORK_DIR=${OUTPUT_DATA_DIR}/work/${stream}

# Only set IN_DIR and OUT_DIR if not already set
export OUT_DIR=${OUT_DIR:-${WORK_DIR}/output}
export IN_DIR=${IN_DIR:-${WORK_DIR}/input}

# Setup reprocessing scripts
export PATH=$PATH:$PIPELINESCRIPTS

# Launch the script
export SCRIPT=${IN2P3_SCRIPT_LOCATION}/${PIPELINE_PROCESS:-$1}

# set -xe; export SHELLOPTS;
source ${SCRIPT}
