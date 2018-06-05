# SRS DRP pipeline files

## Data and software directories

### @CC-IN2P3

Here is an attempt to organise the data and softwares at CC-IN2P3:

- Raw data : `/sps/lsst/datasets/desc/DC2/Run?.?`
- Production data set: `/sps/lsst/dataproducts/desc/DC2/Run?.?`
- Dedicated Pipeline software: `/pbs/throng/lsst/software/desc/DC2/Run?.?`
- DM stack: `/cvmfs/sw.lsst.eu/linux-x86_64/lsst_distrib/w_2018_??`

In each `DC2` folders will be found several sub-folders corresponding
to the different runs carried out during the test and production
phases (Run1.1, Run1.2, Run1.etc) The `dedicated software` folder and
its content will only be modifiable by a handful of people (Dominique,
Johann, Fabio, Nicolas) through the modification of its Access Control
List (ACL).

### @NESRC

- Raw data : `/global/projecta/projectdirs/lsst/production/DC2/`
- Raw Data for transfer to IN2P3 : `/global/projecta/projectdirs/lsst/global/DC2`
- Production data set : `/global/projecta/projectdirs/lsst/production/DC2/DRP`
- Dedicated Pipeline software : `/global/homes/d/descdm/dc2/drp`
- DM stack Haswell : `/global/common/software/lsst/cori-haswell-gcc/stack/?`
- DM stack KNL : `/global/common/software/lsst/cori-knl-gcc/stack/?`
- DM stack Shifter : TBD

## Structure of this directory

Here is the structure and content of this directory:

    .
    ├── dm_configs    -> configuration files for DM tasks such as processEimage.py
    ├── pipe_scripts  -> utility scripts used to organize the work flow (e.g., create a list of file) 
    ├── pipe_setups   -> utility scripts to setup the different steps of the work flow
    ├── pipe_streams  -> configuration files used for the different streams
    └── README.rst    -> this file


## Pipeline

The step by step instructions to run the pipeline are described
[here](https://github.com/LSSTDESC/ImageProcessingPipelines/wiki/Step-by-step-instructions-for-initial-cross-check-of-DM-DC2). After
validation, each step has been incrementaly added to the pipeline to
build the tasks mentioned below.

### Incremental data processing

We are now running in a incremental mode, which means that when more
data will become available to process (the remaining data of Run1.2p
for instance), we will be able to increment on what has been already
done. To do so, 5 separated tasks are being used:

- `DC2DM_1_INGEST`, for data ingestion;
- `DC2DM_2_PROCESS`, to run `processEimage`; 
- `DC2DM_3_FPSUM`, to run `makeFpSummary`;
- `DC2DM_4_COADD`, to run the coadd steps;
- `DC2DM_5_FORCEDPHOT`, to run the forced photometry (on CCDs and coadds).

All these tasks can be found on the [SRS Pipeline2 web
inerface](http://srs.slac.stanford.edu/Pipeline-II/exp/LSST-DESC/index.jsp?versionGroup=latestVersions&submit=Filter&d-4021922-s=1&d-4021922-o=2&taskFilter=DC2DM_&include=last30).

### Outputs

For a iven production campaing, i.e., for a given data set (RUN) and
version of the stack (DMVER), all the outputs of the incremental
processing can be found at the following location at CC-IN2P3

    /sps/lsst/dataproducts/desc/DC2/RUN/DMVER/data

For instance, the output data for the `Run1.2p` can be found at

    /sps/lsst/dataproducts/desc/DC2/Run1.2p/w_2018_18/data

The `/sps/lsst/dataproducts/desc/DC2/RUN/DMVER/` directory will
contain the following files and directories

- the `filesToIngest_*.txt` files, containing the list of fits files
 to ingest, all created by the `createIngestFileList.py` script (see
 below)
- the list of `stream_config_*.sh` files, given as an option when
  creating a stream (`LOCAL_CONFIG=/path/to/this/file.sh`
- the `checkpoints.sh` file, used to stop the pipeline if needed (by
 uncommenting the right process name)
- the `work` directory, containing the scripts and files created
 in the different streams by the setup processes
- the `data` directory, containing the output of the DM stack
procesing for all the streams (the actual data and catalogs).

### How to increment

To increment on the data ingestion, a new list of file can be created using
the following command line:

    createIngestFileList.py VISITDIR --recursive --increment

that needs to be run in the directory where it has been run before (it
will look for the lists previously created). Depending on the number
of files to ingest, this script, that can be foud in the
(pipe_scripts)[pipe_scripts] directory, will create several lists with
a maximum number of 500.000 files to ingest in each. One stream will
have to be created for each of them, one after the other, using
separated `LOCAL_CONFIG` (`stream_config.sh`) file.

When the ingestion is done, the `processEimage` step can be launch
(create a new stream for task `DC2DM_2_PROCESS`, with the same
`LOCAL_CONFIG` file as for the first ingest). This step is set up to
automatically detect new files to process. It makes a direct
comparison between the `eimage` available and the `calexp` already
produced. When the `calexp` are created, you can created a new stream
for the `DC2DM_3_FPSUM` task to run `makeFpSummary` is needed.

The co-addition and the forced-photometry steps are done in two other
tasks. To run the coadd, we might want to wait for a full filter to be
ready (all data available), and we can run one stream per filter. To
run the forced photometry, we will have to wait for all previous steps
to be finished for all filters.

### Setup the pipeline

A description on how to setup the pipeline files and directories
(@CC-IN2P3) has been written
[there](https://github.com/LSSTDESC/ImageProcessingPipelines/tree/master/workflows/srs/pipe_setups/in2p3_setup).





