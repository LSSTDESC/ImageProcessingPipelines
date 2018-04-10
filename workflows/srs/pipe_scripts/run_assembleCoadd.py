#!/usr/bin/env python


from __future__ import print_function
import numpy as N
import libRun as LR
import os

def build_cmd(patches, config, filt, input, output):
    cmd = "assembleCoadd.py %s --output %s " % (input, output) + \
          patches + " @" + filt + ".list" + " --configfile " + config + \
          " --clobber-config"
    print("\nCMD:", cmd)
    return cmd


if __name__ == "__main__":

    usage = """%prog [option]"""

    description = """This script will run makeCoaddTempExp for a given list of filters and patches.
    The  default if to use f.list files (where 'f' is a filter in ugriz) and patches_f.txt, 
    and launch assembleCoadd in several batch jobs. You thus need to be running it at CC-IN2P3 to make 
    it work. To run all  filters, you can do something like 
    %prog -f ugriz -m 1 -c makeCoaddTempExpConfig.py -a"""

    opts, args = LR.standard_options(usage=usage, description=description)

    # overwrite the -m option to force it to be 1
    opts.mod = 5
    opts.input = "pardir/output"
    opts.output = "pardir/output"

    # Loop over filters
    for filt in opts.filters:

        config = LR.select_config(opts.configs, filt)

        # Are there visits to load
        if not os.path.exists("patches_" + filt + ".txt"):
            print("WARNING: No file (no visit) for filter", filt)
            continue

        # Get the list of patches
        patches = [" ".join(p) for p in N.loadtxt("patches_" + filt + ".txt",
                                                  dtype='bytes').astype('str')]
        print("INFO: %i patches loaded: " % len(patches))

        # How many jobs should we be running (and how many visit in each?)?
        njobs = LR.job_number(patches, opts.mod, opts.max)

        # Reorganize the visit list in consequence
        patches = LR.organize_items(patches, njobs)

        # Loop over the patches sub lists
        for i, ps in enumerate(patches):

            # Build the command line and other things
            cmd = build_cmd("@scripts/%s/patches_%03d.list" % (filt, i + 1),
                            config, filt, opts.input, opts.output)

            # Only submit the job if asked
            LR.submit(cmd, "patches_%03d" % (i + 1), filt, autosubmit=opts.autosubmit,
                      ct=1000, vmem='4G', from_slac=opts.fromslac)

            N.savetxt("scripts/%s/patches_%03d.list" % (filt, i + 1),
                      N.array(ps, dtype='str').tolist(), fmt="%s")

    if not opts.autosubmit:
        print("\nINFO: Use option --autosubmit to submit the jobs")
