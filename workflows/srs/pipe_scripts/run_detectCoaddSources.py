#!/usr/bin/env python


from __future__ import print_function
import os
import time
import glob
import libRun as LR


def build_cmd(patch, configFile, filt, input, output):
    if not os.path.isdir("scripts/" + filt):
        os.makedirs("scripts/" + filt)

    cmd = "detectCoaddSources.py %s --output %s " % (input, output) + \
          patch + " --configfile " + configFile + " --clobber-version"
    return cmd


if __name__ == "__main__":

    usage = """%prog [option]"""

    description = """This script will run detectCoadSources for a given list of filters and patches. 
    The  default if to use f.list files (where 'f' is a filter in ugriz) and patches_f.txt, 
    and launch detectCoadSources in several batch jobs. You thus need to be running it at CC-IN2P3 
    to make it work. To run all  filters, you can do something like 
    %prog -f ugriz -m 1 -c detectCoaddSources.py -a"""

    opts, args = LR.standard_options(usage=usage, description=description)

    opts.input = "pardir/output"
    opts.output = "pardir/output"

    # Loop over filters
    for filt in opts.filters:

        config = LR.select_config(opts.configs, filt)

        # Are there visits to load
        if not os.path.exists("patches_" + filt + ".txt"):
            print("WARNING: No file (no visit) for filter", filt)
            continue

        # Build the command line and other things
        cmd = build_cmd("@patches_%s.txt" % filt, config,
                        filt, opts.input, opts.output)

        # Only submit the job if asked
        prefix = "patches_%s" % filt
        LR.submit(cmd, prefix, filt, autosubmit=opts.autosubmit,
                  ct=5000, vmem='4G', from_slac=opts.fromslac)

    if not opts.autosubmit:
        print("\nINFO: Use option --autosubmit to submit the jobs")
