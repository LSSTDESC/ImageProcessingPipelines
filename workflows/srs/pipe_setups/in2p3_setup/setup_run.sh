# To use this script, the follwoing set of environment variable first need to be set in you session
# export DATASET="Run1.2p"     # name of the dataset directory in /sps/lsst/datasets/desc/DC2/
# export RUN="Run1.2p"         # name the you want to give to your run 
# export DMVER="w_2018_15"     # version of the DM stack that you want to use

## Software setup

# Move to the software directory
export SOFTDIR=/pbs/throng/lsst/software/desc/DC2
cd $SOFTDIR

# Create a subdirectory for the current run
echo "Creating directory $SOFTDIR/$RUN"
mkdir $RUN
cd $RUN

# Clone and install the `ImageProcessingPipelines` and `obs_lsstSim` packages
echo
echo "cloning ImageProcessingPipelines into $SOFTDIR/$RUN"
git clone https://github.com/LSSTDESC/ImageProcessingPipelines.git
echo "cloning obs_lsstSim into $SOFTDIR/$RUN"
git clone https://github.com/LSSTDESC/obs_lsstSim.git
echo
echo "Installing obs_lsstSim"
cd obs_lsstSim
source /cvmfs/sw.lsst.eu/linux-x86_64/lsst_distrib/$DMVER/loadLSST.bash
setup lsst_distrib
setup -k -r .
scons

echo
echo "Done with software install"
echo "Moving to output data directory setup"

## Data dirctory setup

# Move to the output data directory
export DATADIR=/sps/lsst/dataproducts/desc/DC2/
cd $DATADIR

# Create a subdirectory for the current run
echo
echo "Creating directory $DATADIR/$RUN"
mkdir $RUN
cd $RUN

# Create a subdirectory for the versino of the stack that will be used
echo "Creating directory $DATADIR/$RUN/$DMVER"
mkdir $DMVER
cd $DMVER

# Make local copies of files needed by the pipeline
echo "Making local copies of checkpoints.sh and stream_config.sh"
cp $SOFTDIR/$RUN/ImageProcessingPipelines/workflows/srs/pipe_setups/checkpoints.sh .
cp $SOFTDIR/$RUN/ImageProcessingPipelines/workflows/srs/pipe_streams/stream_config.sh .

# Edit the stream_config.sh file
echo "Editing stream_config.sh"
sed -i s/DM_RELEASE=/DM_RELEASE=\"$DMVER\"/g stream_config.sh
sed -i s/RUN=/RUN=\"$RUN\"/g stream_config.sh

# Create the list of file to ingest
echo
echo "Now creating the list of file to ingest"
export PATH=$PATH:$SOFTDIR/$RUN/ImageProcessingPipelines/workflows/srs/pipe_scripts/
export VISITDIR=/sps/lsst/datasets/desc/DC2/$DATASET/
createIngestFileList.py $VISITDIR --recursive

echo
echo "Setup is now done. Here is the list of data and software directories"
echo " - Raw data : /sps/lsst/datasets/desc/DC2/$DATASET"
echo " - Production data dir: $DATADIR/$RUN/$DMVER"
echo " - Dedicated Pipeline softwares: $SOFTDIR/$RUN"
echo " - DM stack: /cvmfs/sw.lsst.eu/linux-x86_64/lsst_distrib/$DMVER"
