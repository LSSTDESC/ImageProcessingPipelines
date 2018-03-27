#!/usr/bin/env python

"""
.. _run_forcedPhotCcd:

Run forcedPhotCcd.py for a list of visits
======================================
"""


from __future__ import print_function
import os
import numpy as N
import libRun as LR


__author__ = 'Nicolas Chotard <nchotard@in2p3.fr>'
__version__ = '$Revision: 1.0 $'


def build_cmd(visit, config, filt, input='pardir/input', output='pardir/output'):

    # Create the command line
    cmd = "forcedPhotCcd.py %s --output %s " % (input, output) + \
          "@scripts/%s/%s.txt" % (filt, visit) + " --configfile " + config

    return cmd


if __name__ == "__main__":

    usage = """%prog [option]"""

    description = """This script will run forcedPhotCcd for a given list of filters and visits. The 
    default if to use f.list files (where 'f' is a filter in ugriz), and launch forcedPhotCcd in 
    several batch jobs. You thus need to be running it at CC-IN2P3 to make it work. To run all 
    filters, you can do something like %prog -f ugriz -m 1 -c forcedPhotCcdConfig.py -a
    """

    opts, args = LR.standard_options(usage=usage, description=description)

    opts.mod = 1
    opts.input = "pardir/output"
    opts.output = "pardir/output"

    # Loop over filters
    for filt in opts.filters:
        if not os.path.exists("%s.list" % filt):
            print("WARNING: No visit for filter %s, skip it" % filt)
            continue

        if not os.path.isdir("scripts/" + filt):
            os.makedirs("scripts/" + filt)

        # Get the list of visits
        visits = N.loadtxt(filt+".list", dtype='str')
        print("\nINFO: %i visits loaded for %s: " % (len(visits), filt))

        # Loop over the visit sub lists
        for i, vs in enumerate(visits):

            vs = " ".join(vs)
            prefix = "visit_%03d" % (i + 1)
            N.savetxt("scripts/%s/%s" % (filt, prefix + ".txt"), [str(vs)], fmt="%s")

            # Build the command line and other things
            cmd = build_cmd(prefix, opts.configs, filt, opts.input, opts.output)

            # Only submit the job if asked
            LR.submit(cmd, prefix, filt, autosubmit=opts.autosubmit,
                      ct=opts.ct, vmem=opts.vmem, queue=opts.queue,
                      system=opts.system, otheroptions=opts.otheroptions,
                      from_slac=opts.fromslac)

    if not opts.autosubmit:
        print("\nINFO: Use option --autosubmit to submit the jobs")
