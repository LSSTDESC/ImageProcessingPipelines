#!/usr/bin/env python

"""
.. _run_processEimage:

Run processEimage.py for a list of visits
=========================================
"""

from __future__ import print_function
import os
import numpy as N
import libRun as LR


__author__ = 'Nicolas Chotard <nchotard@in2p3.fr>'
__version__ = '$Revision: 1.0 $'


def build_cmd(visit, config, filt, input='pardir/input', output='pardir/output'):

    if not os.path.isdir("scripts/" + filt):
        os.makedirs("scripts/" + filt)

    # Create the command line
    cmd = "export PATH=$PATH:$SCRIPT_LOCATION\n"
    for vis in visit:
        cmd += "reportPatchesWithImages.py %s --visits %s --filt %s\n" % (input, vis, filt)
    print("\nCMD: ", cmd)

    return cmd

    
if __name__ == "__main__":

    usage = """%prog [option]"""

    description = """This script will run processEimage for a given list of filters and visits. The 
    default if to use f.list files (where 'f' is a filter in ugriz), and launch processEimage in 
    several batch jobs. To run all filters, you can do something like 
    
    %prog -f ugriz -m 1 -c processConfig.py -a

    """

    opts, args = LR.standard_options(usage=usage, description=description)

    # Loop over filters
    for filt in opts.filters:

        config = LR.select_config(opts.configs, filt)

        # Are there visits to load
        if not os.path.exists(filt+"_visit.list"):
            print("WARNING: No file (no visit) for filter", filt)
            continue

        # Get the list of visits
        allvisits = N.loadtxt(filt+"_visit.list", dtype='str', unpack=True)
        if isinstance(allvisits[1], str):
            allvisits = [allvisits[1]]
        else:
            allvisits = allvisits[1]
        visits = [visit.split('=')[1].strip("'") for visit in allvisits]
        print("INFO: %i visits loaded: " % len(visits), visits)

        # How many jobs should we be running (and how many visit in each?)?
        njobs = LR.job_number(visits, opts.mod, opts.max)

        # Reorganize the visit list in sequence
        visits_lists = LR.organize_items(visits, njobs)

        # Loop over the visit sub lists
        numscript = 1
        for i, visits in enumerate(visits_lists):
            cmd = build_cmd(visits, config, filt, input=opts.input, output=opts.output)
            
            # Only submit the job if asked
            prefix = "visit_%03d_script" % numscript
            LR.submit(cmd, prefix, filt, autosubmit=opts.autosubmit,
                      ct=opts.ct, vmem=opts.vmem, queue=opts.queue,
                      system=opts.system, otheroptions=opts.otheroptions,
                      from_slac=opts.fromslac, from_nersc=opts.fromnersc)
            numscript += 1    
                    

    if not opts.autosubmit:
        print("\nINFO: Use option --autosubmit to submit the jobs")
