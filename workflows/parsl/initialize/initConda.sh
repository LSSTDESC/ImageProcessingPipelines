#!/bin/bash -e
## initWorkflow.sh - setup the bits needed for a workflow environment

## The following must be done *just once*
module load python/3.7-anaconda-2019.07
conda create --name=parsl-lsst-dm python=3.7
source activate parsl-lsst-dm
conda install pip
pip install 'parsl[monitoring] @ git+https://github.com/parsl/parsl@lsst-dm-202005'

## Primarily for wstat
conda install matplotlib
conda install tabulate

## Note that to *only* update Parsl (with a given branch), one can
## omit the above and simply:
##
## 1. Obtain the proper workflow environment
## 2. $ pip install --upgrade 'parsl[monitoring] @ git+https://github.com/parsl/parsl@lsst-dm-202002'
