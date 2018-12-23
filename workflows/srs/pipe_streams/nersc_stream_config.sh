# Default location from which to setup DM 
export DM_RELEASE=w_2018_48
export RUN=Run2.1i
export DM_SETUP="/cvmfs/sw.lsst.eu/linux-x86_64/lsst_distrib/${DM_RELEASE}/loadLSST.bash"
export
REFCAT="/global/projecta/projectdirs/lsst/groups/SSim/DC2/reference_catalogs/Run2.0/ref_cats"
#export REFCATCONFIG="/global/cscratch1/sd/descdm/DC2/IngestIndexedReferenceTask_DC2-proto.py"

# Base directory for input and output data
export CHECKPOINTS="/global/cscratch1/sd/descdm/DC2/$RUN/${DM_RELEASE}/checkpoints_run2.1i.sh"
export VISIT_FILE="/global/cscratch1/sd/descdm/DC2/$RUN/${DM_RELEASE}/filesToIngest.txt"
export OUTPUT_DATA_DIR="/global/cscratch1/sd/descdm/DC2/$RUN/${DM_RELEASE}/"

# The filters
export FILTERS="u,g,r,i,z,y"
