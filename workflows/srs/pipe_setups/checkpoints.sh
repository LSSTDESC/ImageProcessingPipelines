#!/bin/bash

# Checkpoints for scripts

# This script is an example of what your check point script should look like
# The check point names should match the one from the XML file, e.g., 
#   <variables>
#    <var name="checkpoint">validateDrp</var>
#   </variables>
# in the "checkpoint_VD" process
# Use "exit 1" to make it fail 0r "exit 0" to make it pass automatically

# Use this script as a template, and use the env. var. $CHECKPOINTS to point to
# your version of this script. Define it in you config file.

case $checkpoint in
    "validateDrp") exit 0;;
esac
