# EDIT ME
export RUN="Run1.2p"
export DMVER="w_2018_15"
cp /pbs/throng/lsst/software/desc/DC2/$RUN/ImageProcessingPipelines/workflows/srs/pipe_setup/checkpoints.sh .
cp /pbs/throng/lsst/software/desc/DC2/$RUN/ImageProcessingPipelines/workflows/srs/pipe_streams/stream_config.sh .
source /sps/lsst/software/lsst_distrib/${DMVER}/loadLSST.bash
export PATH=$PATH:/pbs/throng/lsst/software/desc/DC2/$RUN/ImageProcessingPipelines/workflows/srs/pipe_script/
export VISITDIR=/sps/lsst/datasets/desc/DC2/$RUN/
createIngestFileList.py $VISITDIR --recursive
