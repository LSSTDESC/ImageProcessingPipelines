# Default location from which to setup DM 
export DM_RELEASE=w_2018_48
export RUN=Run2.0i
#`export DM_SETUP="/cvmfs/sw.lsst.eu/linux-x86_64/lsst_distrib/${DM_RELEASE}/loadLSST.bash"
export DM_SETUP="/global/common/software/lsst/cori-haswell-gcc/DC2/setup_srs_48.sh"
export REFCAT="/global/projecta/projectdirs/lsst/groups/SSim/DC2/reference_catalogs/Run2.0/ref_cats"
#export REFCATCONFIG="/global/cscratch1/sd/descdm/DC2/IngestIndexedReferenceTask_DC2-proto.py"

# Base directory for input and output data
export CHECKPOINTS="/global/cscratch1/sd/descdm/DC2/$RUN/${DM_RELEASE}_test/checkpoints_run2.1i.sh"
#export VISIT_FILE="/global/cscratch1/sd/descdm/DC2/$RUN/${DM_RELEASE}_test/filesToIngest.txt"
export VISIT_FILE="/global/cscratch1/sd/descdm/DC2/$RUN/visit_list_test.txt"
export OUTPUT_DATA_DIR="/global/cscratch1/sd/descdm/DC2/$RUN/${DM_RELEASE}_test/"

# The filters
export FILTERS="u,g,r,i,z,y"
