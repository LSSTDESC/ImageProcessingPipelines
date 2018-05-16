# Default location from which to setup DM 
export DM_RELEASE=
export RUN=
export DM_SETUP="/sps/lsst/software/lsst_distrib/${DM_RELEASE}/loadLSST.bash"
export REFCAT="/sps/lsst/users/nchotard/dc2/test/input/ref_cats"
export REFCATCONFIG="/sps/lsst/users/lsstprod/desc/DC2-test/IngestIndexedReferenceTask_DC2-proto.py"

# Base directory for input and output data
export CHECKPOINTS="/sps/lsst/dataproducts/desc/DC2/$RUN/${DM_RELEASE}/checkpoints.sh"
export VISIT_FILE="/sps/lsst/dataproducts/desc/DC2/$RUN/${DM_RELEASE}/filesToIngest.txt"
export OUTPUT_DATA_DIR="/sps/lsst/dataproducts/desc/DC2/$RUN/${DM_RELEASE}/"

# The filters
export FILTERS="u,g,r,i,z,y"
