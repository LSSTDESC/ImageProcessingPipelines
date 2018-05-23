#!/usr/bin/env python

"""
.. _run_makeFpSummary:

Run makeFpSummary.py for a list of visits
=========================================
"""

from __future__ import print_function
import os
import glob
import numpy as N
import libRun as LR


__author__ = 'Nicolas Chotard <nchotard@in2p3.fr>'
__version__ = '$Revision: 1.0 $'


def build_cmd(visit, filt, num, input='pardir/output', output='pardir/output'):

    if not os.path.isdir("scripts/" + filt):
        os.makedirs("scripts/" + filt)

    filename = "scripts/" + filt + "/visits_" + str(num) + ".list"
    N.savetxt(filename, visit, fmt="%s")
    
    cmd = ""
    # Create the command line
    if opts.time:
        cmd += "time "
    cmd += "makeFpSummary.py %s --output %s --dstype calexp @" % (output, output) + \
           filename
    if opts.showconfig:
        cmd += " --show=config"
    if opts.clobberversions:
        cmd += " --clobber-versions"
    print("\nCMD: ", cmd)

    return cmd

    
if __name__ == "__main__":

    usage = """%prog [option]"""

    description = """This script will run makeFpSummary for a given list of filters and visits. The 
    default if to use f.list files (where 'f' is a filter in ugriz), and launch makeFpSummary in 
    several batch jobs. To run all filters, you can do something like 
    
    %prog -f ugriz -m 1 -c processConfig.py -a

    """

    opts, args = LR.standard_options(usage=usage, description=description)

    # Loop over filters
    for filt in opts.filters:

        config = LR.select_config(opts.configs, filt)

        # Are there visits to load
        if not os.path.exists(filt+".list"):
            print("WARNING: No file (no visit) for filter", filt)
            continue

        # Get the list of visits
        dataids = N.loadtxt(filt+".list", dtype='str')
        visits = list(set([dataid[1].split('=')[1] for dataid in dataids]))
        print("INFO: %i visits loaded: " % len(visits), visits)

        # How many jobs should we be running (and how many visit in each?)?
        njobs = LR.job_number(visits, opts.mod, opts.max)

        # Reorganize the visit list in sequence
        visits = LR.organize_items(visits, njobs)

        # Loop over the visit file
        numscript = 1
        for i, visit in enumerate(visits):
            cmd = build_cmd(visit, filt, i, input=opts.output, output=opts.output)

            # Only submit the job if asked
            prefix = "visit_makeFpSummary_%03d_script" % numscript
            LR.submit(cmd, prefix, filt, autosubmit=opts.autosubmit,
                      ct=opts.ct, vmem=opts.vmem, queue=opts.queue,
                      system=opts.system, otheroptions=opts.otheroptions,
                      from_slac=opts.fromslac, from_nersc=opts.fromnersc)
            numscript += 1    

    if not opts.autosubmit:
        print("\nINFO: Use option --autosubmit to submit the jobs")
