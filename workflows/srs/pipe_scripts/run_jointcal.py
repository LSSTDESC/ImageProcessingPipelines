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
import glob


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

    # How many jobs should we be running (and how many tract in each?)?
    njobs = LR.job_number(tracts, opts.mod, opts.max)

    tracts_visits = {}
    for tract in tracts:
        tracts_visits[tract] = {}
        for filt in opts.filters:
            tracts_visits[tract][filt] = []
            flist = glob.glob(filt + '_*_patches.list')
            for clist in flist:
                ctracts = [tr.split('=')[1]
                           for tr in np.loadtxt(clist, dtype='bytes').astype(str)[:, 0]]
                if tract in list(set(ctracts)):
                    tracts_visits[tract][filt].append(clist.split('_')[1])

    # Reorganize the tract list in sequence
    alltracts = LR.organize_items(tracts, njobs)

    # Loop over filters
    for filt in opts.filters:
        for i, tracts in enumerate(alltracts):
            cmd = ""
            for tract in tracts:
                newfile = open('%s_%s.list' % (filt, str(tract)), 'w')
                for visit in tracts_visits[tract][filt]:
                    newfile.write('--id tract=%s visit=%s\n' % (str(tract), str(visit)))
                newfile.close()
                cmd += "jointcal.py %s --output %s @%s_%s.list --configfile %s --clobber-versions -L DEBUG\n" % \
                       (input, output, filt, str(tract), config)

            # Only submit the job if asked
            prefix = "jointcal_%s_%03d" % (filt, i + 1)
            LR.submit(cmd, prefix, filt, autosubmit=opts.autosubmit,
                      ct=opts.ct, vmem=opts.vmem, queue=opts.queue,
                      system=opts.system, otheroptions=opts.otheroptions,
                      from_slac=opts.fromslac)

    if not opts.autosubmit:
        print("\nINFO: Use option --autosubmit to submit the jobs")
