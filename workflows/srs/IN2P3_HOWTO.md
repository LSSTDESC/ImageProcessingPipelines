# How to setup and run the pipeline from CC-IN2P3

## Data and software directories

### Software setup

- This setup has to be done when a new set of data is available under `/sps/lsst/datasets/desc/DC2/`, e.g.,

  /sps/lsst/datasets/desc/DC2/Run1.2p
  
- Go to `/pbs/throng/lsst/software/desc/DC2`
- Create a directory corresponding to the new data set

  mkdir Run1.2p
  export SOFTDIR=/pbs/throng/lsst/software/desc/DC2/Run1.2p
  cd Run1.2p
  
- Clone the `ImageProcessingPipelines` and `obs_lsstSim` packages

  git clone https://github.com/LSSTDESC/ImageProcessingPipelines.git
  git clone https://github.com/LSSTDESC/obs_lsstSim.git

- Install the `obs_lsstSim` package

  cd obs_lsstSim
  source /sps/lsst/software/lsst_distrib/w_2018_15/loadLSST.bash
  setup lsst_distrib
  setup -k -r .
  scons

The `eups declare` will be done by the pipeline for each job.

### Output data directory setup

After creating the software directory, you need to create the output data directory

      cd /sps/lsst/dataproducts/desc/DC2/
      mkdir Run1.2p
      cd Run1.2p
      mkdir w_2018_15
      cd w_2018_15
      cp $SOFTDIR/ImageProcessingPipelines/workflows/srs/pipe_setups/setup_datadir.sh .

- Edit the setup file
- ./setup_datadir.sh
- Edit the stream_config.sh file

### Launch the pipeline



## Pipeline







