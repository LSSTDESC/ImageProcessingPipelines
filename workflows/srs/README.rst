SRS DRP pipeline files
======================

Data and software directories
-----------------------------

@CC-IN2P3
.........

Here is an attempt to organise the data and softwares at CC-IN2P3:

- Raw data : ``/sps/lsst/datasets/desc/DC2/Run?.?``

- Producted data set: ``/sps/lsst/data/desc/DC2/Run?.?``

- Dedicated software: ``/pbs/throng/lsst/software/desc/DC2/Run?.?``

- DM stack: ``/sps/lsst/software/lsst_distrib/w_2018_??``

In each ``DC2`` folders will be found several sub-folders corresponding
to the different runs carries out during the test and production
phases (Run1.1, Run1.2, Run1.etc) The ``dedicated software`` folder and
its content will only be modifiable by a handful of people (Dominique,
Johann, Fabio, Nicolas) through the modification of its Access Control
List (ACL).

@NESRC
......

Structure of this directory
--------------------------

Here is the structure and content of this directory::

  .
  ├── dm_configs    -> configuration files for DM tasks such as processEimage.py
  ├── pipe_scripts  -> utility scripts used to organize the work flow (e.g., create a list of file) 
  ├── pipe_setups   -> utility scripts to setup the different steps of the work flow
  ├── pipe_streams  -> configuration files used for the different streams
  └── README.rst    -> this file







