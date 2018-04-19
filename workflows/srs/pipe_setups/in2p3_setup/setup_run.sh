# To use this script, the follwoing set of environment variable first need to be set in you session
# export DATASET="Run1.2p"     # name of the dataset directory in /sps/lsst/datasets/desc/DC2/
# export RUN="Run1.2p"         # name the you want to give to your run 
# export DMVER="w_2018_15"     # version of the DM stack that you want to use

## Software setup

# Move to the software directory
export SOFTDIR=/pbs/throng/lsst/software/desc/DC2
cd $SOFTDIR

# Create a subdirectory for the current run
mkdir $RUN
cd $RUN

# Clone and install the `ImageProcessingPipelines` and `obs_lsstSim` packages
git clone https://github.com/LSSTDESC/ImageProcessingPipelines.git
git clone https://github.com/LSSTDESC/obs_lsstSim.git
cd obs_lsstSim
source /sps/lsst/software/lsst_distrib/$DMVER/loadLSST.bash
setup lsst_distrib
setup -k -r .
scons


## Data dirctory setup

# Move to the output data directory
export DATADIR=/sps/lsst/dataproducts/desc/DC2/
cd $DATADIR

# Create a subdirectory for the current run
mkdir $RUN
cd $RUN

# Create a subdirectory for the versino of the stack that will be used
mkdir w_2018_15
cd w_2018_15

# Make local copies of files needed by the pipeline
cp $SOFTDIR/$RUN/ImageProcessingPipelines/workflows/srs/pipe_setups/checkpoints.sh .
cp $SOFTDIR/$RUN/ImageProcessingPipelines/workflows/srs/pipe_streams/stream_config.sh .

# Edit the stream_config.sh file
sed -i s/DM_RELEASE=/DM_RELEASE=\"$DMVER\"/g stream_config.sh

# Create the list of file to ingest
export PATH=$PATH:$SOFTDIR/$RUN/ImageProcessingPipelines/workflows/srs/pipe_scripts/
export VISITDIR=/sps/lsst/datasets/desc/DC2/$DATASET/
createIngestFileList.py $VISITDIR --recursive
