#!/bin/bash -l
#SBATCH -p regular   #Submit to the regular 'partition'
#SBATCH -N 4        #Use 4 node
#SBATCH -t 4:00:00  #Set up to 4 hour time limit
#SBATCH -L SCRATCH   #Job requires $SCRATCH file system
#SBATCH -C haswell   #Use Haswell nodes
#SBATCH --output=/global/homes/h/heatherk/dc1/DC1-rerun/logs/dc1-%j.out

# Set up DC1 environment
cd /global/homes/d/descdm/minidrp/pipe_setups

export CLASSPATH=~desc/jobcontrol/org-srs-jobcontrol-2.1.1-SNAPSHOT-jar-with-dependencies.jar
unset LS_COLORS
export P2_SENDMAIL=/global/homes/b/bvan/bsub/bridge.bash

#export SHIFTER_IMAGE=hepcce/desc_dm:v13_0_DC1_noise_fix

module load java
java org.srs.jobcontrol.pilot.JobControlPilot -p MINIDRPHK -L SCRATCH,projecta -C haswell -u desc "$@"

