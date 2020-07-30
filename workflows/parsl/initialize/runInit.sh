#!/bin/bash

## runInit.sh - run the scripts to initialize a DM repository


## This script is intended to be run interactively, just once and
## ideally from the workflow main directory.

## Define workflow root directory
workflowroot=$(dirname $(realpath $0))

## Define task-specific env-vars
source $workflowroot/configTask.sh

## DM setup
source $workflowroot/cvmfsSetup.sh

## Establish a repository for brighter-fatter generation
$workflowroot/initRepo.sh |tee $workflowroot/initRepo.log

