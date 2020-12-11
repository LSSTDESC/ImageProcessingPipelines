#!/bin/bash -e
## initWorkflow.sh - setup the bits needed for a workflow environment
##                   Complete and fresh install of all needed components


## -------------------------------------------------------------------------------------
## To update only an existing Parsl (with a given branch), one can
## just do this:

## 1. Obtain the proper workflow environment
## 2. $ pip install --upgrade 'parsl[monitoring] @ git+https://github.com/parsl/parsl@lsst-dm-202002'
##

## This scheme ^^^^^ is not always 100% successful.  It is worth
## checking the files match with the appropriate branch/release in
## github.  If not, then you may reinstall Parsl:

## $ pip uninstall parsl
## $ pip install 'parsl[monitoring] @ git+https://github.com/parsl/parsl@lsst-dm-202012'
## -------------------------------------------------------------------------------------



## The following sequence needs be done *just once* (per conda
## environment)

# make desired python version visible
module load python/3.8-anaconda-2020.11

# create a new conda "environment"
conda create --name=parsl-lsst-3.8 python=3.8

#THIS MAY BE NEEDED#conda init bash
# (then restart shell, 'exec bash')

# activate specified conda environment
source activate parsl-lsst-3.8



## Add in needed 3rd party packages
pip install 'parsl[monitoring] @ git+https://github.com/parsl/parsl@lsst-dm-202012'
pip install more-itertools


## These are for Perp (wstat)
conda install matplotlib
conda install tabulate



#################### OBSOLETE as of 12/2/2020 ##########################
#retired 20201202#module load python/3.7-anaconda-2019.07
#retired 20201202#conda create --name=parsl-lsst-dm python=3.7
#source activate parsl-lsst-dm
#retired 20201202#pip install 'parsl[monitoring] @ git+https://github.com/parsl/parsl@lsst-dm-202005
#conda install pip      ###  <--- no longer necessary!
#################### OBSOLETE as of 12/2/2020 ##########################
