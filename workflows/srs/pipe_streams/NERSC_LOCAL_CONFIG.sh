# Default location from which to setup DM
export DM_RELEASE="w_2018_09"
export DM_SETUP_SCRIPT="setup_w_2018_09-sims_2_6_0.sh"
export DM_SETUP="/global/common/software/lsst/cori-haswell-gcc/stack/${DM_SETUP_SCRIPT}"
#export DM_CONFIG="${DM_RELEASE}/lsstSim"

# empty for now - installation area for gcr-catalogs?
export PIPELINESCRIPTS=""

# Base directory for input and output data
export VISIT_FILE="/global/cscratch1/sd/descdm/DC2/dc2_test/filesToIngest.txt"
export OUTPUT_DATA_DIR="/global/cscratch1/sd/descdm/DC2/dc2_test"

# The filters
export FILTERS="u,g,r,i,z,y"
