#!/bin/bash

# Checkpoints for scripts

# This script is an example of what your check point script should look like
# The check point names should match the one from the processes in the XML file, e.g., 
#   $PIPELINE_PROCESS == "runDetectCoaddSources"
#
# Use "exit 1" to make it fail, "exit 0" to make it pass automatically, or echo a "No check point"
# message if you want to run the script without any intervention (or comment out the process name
# from the list)

# Use this script as a template, and use the env. var. $CHECKPOINTS to point to
# your version of this script. Define it in you config file.

case $PIPELINE_PROCESS in
    # uncomment the script you would like to stop
#    "ingestData") exit 1;;
#    "ingestRefCat") exit 1;;
#    "makeSkyMap") exit 1;;
#    "setup_assembleCoadd") exit 1;;
#    "setup_detectCoaddSources") exit 1;;
#    "setup_forcedPhotCcd") exit 1;;
#    "setup_forcedPhotCoadd") exit 1;;
#    "setup_ingest") exit 1;;
#    "setup_jointcal") exit 1;;
#    "setup_jointcalCoadd") exit 1;;
#    "setup_makeFpSummary") exit 1;;
#    "setup_measureCoaddSources") exit 1;;
#    "setup_mergeCoaddDetections") exit 1;;
#    "setup_mergeCoaddMeasurements") exit 1;;
#    "setup_processEimage") exit 1;;
#    "setup_reportPatches") exit 1;;
#    "validateDrp") exit 1;;
#    "wrapPatchesLists") exit 1;;
esac
