#!/usr/bin/env python

"""
.. _run_jointcal:

Run jointcal.py for a list of visits
======================================
"""


from __future__ import print_function
import libRun as LR
import numpy as np
import os


__author__ = 'Nicolas Chotard <nchotard@in2p3.fr>'
__version__ = '$Revision: 1.0 $'


if __name__ == "__main__":

    usage = """%prog [option]"""

    description = """Run jointcal for a given list of filters"""

    opts, args = LR.standard_options(usage=usage, description=description)

    input = "pardir/output"
    output = "pardir/output"
    config = "jointcalConfig.py"

    patches = np.loadtxt('patches.txt', dtype='str', unpack=True)
    tracts = [s.split('=')[1] for s in set(patches[0])]
    # Loop over filters
    for filt in opts.filters:
        # Are there visits to load
        if not os.path.exists('%s.list' % filt):
            print("WARNING: No file (no visit) for filter", filt)
            continue
        cmd = ""
        for tract in tracts:
            lfile = open('%s.list' % filt, 'r')
            lines = lfile.readlines()
            lfile.close()
            newfile = open('%s_%s.list' % (filt, str(tract)), 'w')
            for line in lines:
                newfile.write(line.replace('--id ', '--id tract=%s ' % str(tract)))
            newfile.close()
            cmd += "jointcal.py %s --output %s @%s_%s.list --configfile %s\n" % \
                   (input, output, filt, str(tract), config)
        # Only submit the job if asked
        prefix = "jointcal_%s" % filt
        LR.submit(cmd, prefix, filt, autosubmit=opts.autosubmit,
                  ct=opts.ct, vmem=opts.vmem, queue=opts.queue,
                  system=opts.system, otheroptions=opts.otheroptions,
                  from_slac=opts.fromslac)

    if not opts.autosubmit:
        print("\nINFO: Use option --autosubmit to submit the jobs")
