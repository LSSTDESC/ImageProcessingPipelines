# Default location from which to setup DM 
export DM_RELEASE="w_2018_07"
export DM_SETUP="/sps/lsst/software/lsst_distrib/${DM_RELEASE}/loadLSST.bash"
export DM_CONFIG="${DM_RELEASE}/lsstSim"
export PIPELINESCRIPTS="/pbs/throng/lsst/software/desc/DC2/Run1.1"

# Base directory for input and output data
export VISIT_DIR="/sps/lsst/data/clusters/workflow/dc2_test/DC2-phoSim-3_WFD-r.txt"
export OUTPUT_DATA_DIR="/sps/lsst/data/clusters/workflow/dc2_test"

# The filters
export FILTERS="u,g,r,i,z,y"
