#!/bin/bash
## runWorkflow.sh - setup env and run DRP workflow

## The following runs the workflow
echo "source ./setup.source"
source ./setup.source

echo "./workflow.py"
./workflow.py
