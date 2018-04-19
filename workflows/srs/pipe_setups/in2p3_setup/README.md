# How to setup and run the pipeline from CC-IN2P3

## Data and software directories setup

Define the following envirnoment variables that will be needed by the setup scripts.

      export DATASET="Run1.2p"     # name of the dataset directory in /sps/lsst/datasets/desc/DC2/
      export RUN="Run1.2p"         # name the you want to give to your run 
      export DMVER="w_2018_15"     # version of the DM stack that you want to use

Get the setup script from github

      wget https://raw.githubusercontent.com/LSSTDESC/ImageProcessingPipelines/master/workflows/srs/pipe_setups/in2p3_setup/setup_run.sh
      chmod 755 setup_run.sh

Run the script
    
      ./setup_run.sh

## Launch the pipeline

Go the the SRS [web interface](http://srs.slac.stanford.edu/Pipeline-II/exp/LSST-DESC/index.jsp) . If not already done, add your task `Admin -> Browse -> Upload`. When the task is loaded, launch a new stream in `Create a stream`. Select the task, leave the strem number blanck (it will be increment by default), and give the following argument

        LOCAL_CONFIG=/sps/lsst/dataproducts/desc/DC2/THERUN/THESTACKVERSION/stream_config.sh

replacing `THERUN` and `THESTACKVERSION` by the right value, e.g., `Run1.2p` and `w_2018_15` for this example. 
