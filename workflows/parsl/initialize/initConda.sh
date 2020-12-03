#!/bin/bash -e
## initWorkflow.sh - setup the bits needed for a workflow environment

## Note that to *only* update Parsl (with a given branch), one can
## just do this:
##
## 1. Obtain the proper workflow environment
## 2. $ pip install --upgrade 'parsl[monitoring] @ git+https://github.com/parsl/parsl@lsst-dm-202002'



## The following needs be done *just once* (per conda environment)

module load python/3.8-anaconda-2020.11

conda create --name=parsl-lsst-3.8 python=3.8
#THIS MAY BE NEEDED#conda init bash
# (then restart shell, 'exec bash')

source activate parsl-lsst-3.8

pip install 'parsl[monitoring] @ git+https://github.com/parsl/parsl@lsst-dm-202012'

## Primarily for wstat
conda install matplotlib
conda install tabulate



#################### OBSOLETE as of 12/2/2020 ##########################
#retired 20201202#module load python/3.7-anaconda-2019.07
#retired 20201202#conda create --name=parsl-lsst-dm python=3.7
#source activate parsl-lsst-dm
#retired 20201202#pip install 'parsl[monitoring] @ git+https://github.com/parsl/parsl@lsst-dm-202005
#conda install pip      ###  <--- no longer necessary!
#################### OBSOLETE as of 12/2/2020 ##########################
