# SRS DRP pipeline files

## Data and software directories

### @CC-IN2P3

Here is an attempt to organise the data and softwares at CC-IN2P3:

- Raw data : `/sps/lsst/datasets/desc/DC2/Run?.?`

- Producted data set: `/sps/lsst/dataprod/desc/DC2/Run?.?`

- Dedicated software: `/pbs/throng/lsst/software/desc/DC2/Run?.?`

- DM stack: `/sps/lsst/software/lsst_distrib/w_2018_??`

In each `DC2` folders will be found several sub-folders corresponding
to the different runs carries out during the test and production
phases (Run1.1, Run1.2, Run1.etc) The `dedicated software` folder and
its content will only be modifiable by a handful of people (Dominique,
Johann, Fabio, Nicolas) through the modification of its Access Control
List (ACL).

### @NESRC

- Raw data : `/global/projecta/projectdirs/lsst/production/DC2/`

## Structure of this directory

Here is the structure and content of this directory:

    .
    ├── dm_configs    -> configuration files for DM tasks such as processEimage.py
    ├── pipe_scripts  -> utility scripts used to organize the work flow (e.g., create a list of file) 
    ├── pipe_setups   -> utility scripts to setup the different steps of the work flow
    ├── pipe_streams  -> configuration files used for the different streams
    └── README.rst    -> this file


## Pipeline

The step by step instruction to run the pipeline are for now kept
[here](https://github.com/LSSTDESC/ImageProcessingPipelines/wiki/Step-by-step-instructions-for-initial-cross-check-of-DM-DC2). After
validation, each step is incrementaly added to the pipeline.

## Status for the mini-DRP pipeline

- [ ] Data transfer from NERSC to CC-IN2P3
  - [ ] Creation of a stable directory structure at NERSC
  - [ ] Automatic copy of its content to CC-IN2P3
  - [ ] Identify datasets to exercise mini DRP pipeline
- [x] Creation of a list of data to ingest (`pipe_scripts/createIngestFileList.py`)
- [x] Data ingestion using `ingestDriver.py`
- [ ] Build a reference catalog with stars and galaxies
  - [ ] What software do we need for that?
  - [ ] What configuration?
  - [ ] Some documentation from Dominique [here](https://github.com/LSSTDESC/ImageProcessingPipelines/wiki/How-to-create-the-protoDC2-reference-catalog)
  - Dominique has created these catalogs, that can be found here

    	      /sps/lsst/users/lsstprod/desc/DC2-test/input/ref_cats

- [x] Creation of a list of visits to process (incremental mode available)
- [ ] Run `processEimage.py`
  - [ ] Config file?

      	       in `/sps/lsst/users/lsstprod/desc/DC2-test/processEimage.py`
	       or `/sps/lsst/users/nchotard/dc2/obs_lsstSim/config/processEimage.py`

  - [ ] Version or branch of `obs_lsstSim` to use? We want master.
- [ ] Run `makeFpSummary` to produce control plots for eyeballers
  - For now, available in `u/krughoff/fp_overview` branch of `obs_lsstSim`
  - See details [there](https://github.com/LSSTDESC/ImageProcessingPipelines/wiki/Step-by-step-instructions-for-initial-cross-check-of-DM-DC2#run-makefpsummarypy)
- [ ] Run `validate_drp`?
  






