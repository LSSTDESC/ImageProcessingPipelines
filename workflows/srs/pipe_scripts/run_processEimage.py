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


def build_cmd(visit, config, filt, raft=None, input='pardir/input', output='pardir/output'):

    if not os.path.isdir("scripts/" + filt):
        os.makedirs("scripts/" + filt)

    # Create and save a sub list of visit
    if raft is not None:    
        filename = "scripts/" + filt + "/" + visit + "_R" + raft.replace(',', '') + ".list"
        N.savetxt(filename, ["--id visit=%s raft='%s'" % (visit, raft)], fmt="%s")
    else:
        filename = "scripts/" + filt + "/" + visit + ".list"
        N.savetxt(filename, ["--id visit=%s" % visit], fmt="%s")

    # Create the command line
    cmd = "processEimage.py %s --output %s @" % (input, output) + \
          filename
    if config is not None:
        cmd += " --configfile " + config
    if opts.multicore:
        cmd += " -j 8 --timeout 999999999"
    if opts.doraise:
        cmd += " --doraise"
    cmd += "\n"
    if opts.time:
        cmd += "time "
        cmd = "time " + cmd
    cmd += "makeFpSummary.py %s --output %s @" % (input, output) + \
           filename
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
        if not os.path.exists(filt+".list"):
            print("WARNING: No file (no visit) for filter", filt)
            continue

        # Get the list of visits
        allvisits = N.loadtxt(filt+".list", dtype='str', unpack=True)
        if isinstance(allvisits[1], str):
            allvisits = [allvisits[1]]
        else:
            allvisits = allvisits[1]
        visits = [visit.split('=')[1].strip("'") for visit in allvisits]
        print("INFO: %i visits loaded: " % len(visits), visits)

        # How many jobs should we be running (and how many visit in each?)?
        opts.mod = 1  # one job per visit to be faster
        njobs = LR.job_number(visits, opts.mod, opts.max)

        # Reorganize the visit list in consequence
        visits = LR.organize_items(visits, njobs)

        # specific options for processEimage
        # may not want to set queue long at NERSC
        opts.queue = "long"
        if opts.multicore:
            opts.queue = "mc_huge"
            opts.otheroptions = "-pe multicores 8"

        # We will pur one raft per job, tp make sure it does not get killed
        rafts = ['%i,%i' %(i, j) for i in range(5) for j in range(5)]
        rafts = [raft for raft in rafts if raft not in ['0,0', '0,4', '4,0', '4,4']]

        # Loop over the visit sub lists and the raft list
        numscript = 1
        for i, visit in enumerate(visits):
            if opts.perraft:
                for j, raft in enumerate(rafts):
                    # Build the command line and other things
                    cmd = build_cmd(visit[0], config, filt, raft=raft, opts.input, opts.output)
                    
                    # Only submit the job if asked
                    prefix = "visit_%03d_script" % numscript
                    LR.submit(cmd, prefix, filt, autosubmit=opts.autosubmit,
                              ct=opts.ct, vmem=opts.vmem, queue=opts.queue,
                              system=opts.system, otheroptions=opts.otheroptions,
                              from_slac=opts.fromslac, from_nersc=opts.fromnersc)
                    numscript += 1
            else:
                cmd = build_cmd(visit[0], config, filt, opts.input, opts.output)
                    
                # Only submit the job if asked
                prefix = "visit_%03d_script" % numscript
                LR.submit(cmd, prefix, filt, autosubmit=opts.autosubmit,
                          ct=opts.ct, vmem=opts.vmem, queue=opts.queue,
                          system=opts.system, otheroptions=opts.otheroptions,
                          from_slac=opts.fromslac, from_nersc=opts.fromnersc)
                numscript += 1    
                    

    if not opts.autosubmit:
        print("\nINFO: Use option --autosubmit to submit the jobs")
