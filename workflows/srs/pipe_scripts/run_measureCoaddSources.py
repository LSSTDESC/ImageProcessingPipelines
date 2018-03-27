#!/usr/bin/env python


from __future__ import print_function
import os
import glob
import libRun as LR


def build_cmd(patch, configFile, filt, input, output):
    if not os.path.isdir("scripts/" + filt):
        os.makedirs("scripts/" + filt)

    cmd = "mv "+ patch + " scripts/" + filt
    os.system(cmd)

    cmd = "measureCoaddSources.py %s --output %s" % (input, output) + \
          " @scripts/" + filt + "/" + patch + " --configfile " + configFile
    return cmd


if __name__ == "__main__":

    usage = """%prog [option]"""

    description = """This script will run measureCoaddSources for a given list of filters and visits. 
    The default if to use f.list files (where 'f' is a filter in ugriz), and launch it in 
    several batch jobs. You thus need to be running it at CC-IN2P3 to make it work. To run all 
    filters, you can do something like %prog -f ugriz -m 1 -c measureCoaddSourcesConfig.py -a"""

    opts, args = LR.standard_options(usage=usage, description=description)

    opts.mod = 2
    opts.input = "pardir/output"
    opts.output = "pardir/output"

    for filt in opts.filters:

        # Are there visits to load
        if not os.path.exists("patches_" + filt + ".txt"):
            print("WARNING: No file (no visit) for filter", filt)
            continue
            
        filePatch = "patches_" + filt + ".txt"

        cmd = "split -l " + str(opts.mod) + " -d " + filePatch + " " + filePatch + "_"
        os.system(cmd)

        patchList = glob.glob(filePatch + "_*")

        for patch in patchList:
            cmd = build_cmd(patch, opts.configs, filt, opts.input, opts.output)
            LR.submit(cmd, patch, filt, autosubmit=opts.autosubmit, from_slac=opts.fromslac)
