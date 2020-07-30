## configTask.sh 
##
##
## Define environment variables to set up a DM repo
##
## Most variables defined herein will be prefixed with "PT_"
## ("PipelineTask")
##

## This script *must* be sourced to have any value!
if [[ x$BASH_SOURCE =~ x$0 ]]; then
    echo "You must source this script: $BASH_SOURCE"
    exit
fi

## Needed only until the most recent version of parsl is made part of the DM conda installation
export PATH="'${PATH}:${HOME}'"/.local/bin


#####################################################
###########  Global variables
#####################################################

##     PT_WORKFLOWROOT is where the workflow scripts live (must be same dir as this config script)
export PT_WORKFLOWROOT="$(realpath $(dirname $BASH_SOURCE))"

export PT_SCRATCH='/global/cscratch1/sd/bxc'

##     PT_OUTPUTDIR is the general area where the output goes, e.g., $SCRATCH or projecta
export PT_OUTPUTDIR=$PT_SCRATCH

##      PT_DEBUG is a global flag for workflow development & debugging
export PT_DEBUG=False

##     PT_REPODIR is the location of DM data repository
export PT_REPODIR=${PT_OUTPUTDIR}'/lsst-dm-repo-1'

#####################################################
###########  One-time Setup
#####################################################

### The following values are required to create a working DM-style
### repository (used the initRepo.sh script)

##     PT_CALIBS, PT_REFCAT and PT_BFKERNELS point to calibration products needed by the repo
#export PT_CALIBS='/global/projecta/projectdirs/lsst/production/DC2_ImSim/Run2.1i/CALIB/CALIB_run2-1i-v1.tar.gz'
export PT_CALIBS='/global/cscratch1/sd/descdm/tomTest/tmp/CALIB.tar.gz'

export PT_REFCAT='/global/projecta/projectdirs/lsst/production/DC2_ImSim/Run2.1i/ref_cats/ref_cat-v3/dc2_run2.1i_ref_cats_190513-v3.tar'

export PT_BFKERNELS='/global/projecta/projectdirs/lsst/production/DC2_ImSim/Run2.1i/CALIB/bfkernels-v1/calibrations'

##     PT_INGEST is the (local) file containing a list of all simulated image files to ingest
##               Note: this file must reside in the workflow top-level directory
export PT_INGEST=ingestFileList.txt









#=============  THE FOLLOWING IS NOT USED AT THIS TIME ==============#


###################################################
###########  Production running
###################################################

## singleFrameDriver parameters

##     PT_RERUNDIR is the subdirectory under <repo>/rerun into which results are stored
##                 Note that this value may be adjusted later with a numeric postfix.
export PT_RERUNDIR='20191008'

#export PT_VISITLIST="$PT_WORKFLOWROOT/visitList.txt"  ## Run 2.1i Y3 WFD
export PT_VISITLIST="$PT_WORKFLOWROOT/visitList-2.txt"  ## Run 2.1.1i agn test

export PT_PARALLEL_MAX=10  # "-j" DM parallelization parameter

export PT_NCORES=10        # number of cores one DM tool invocation may use

#----------------------------------------------------------------

## The following is used in association with the Parsl "Config" object

##     PT_ENVSETUP is a script run by the batch script prior to the main event
export PT_ENVSETUP="source ${PT_WORKFLOWROOT}/configTask.sh;export PATH="'${PATH}:${HOME}'"/.local/bin;source ${PT_WORKFLOWROOT}/cvmfsSetup.sh;"



echo;echo;echo
echo "==========================================================================="
## Dump all the "PT_" environent variables to screen
printenv |sort |grep "^PT_"
#echo "  ALL ENVIRONMENT VARIABLES "
## Dump all env-vars for debugging 
#printenv|sort
echo "==========================================================================="
echo;echo;echo
