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

    cmd = "forcedPhotCoadd.py %s --output %s"  % (input, output) + \
          " @scripts/" + filt + "/" + patch + " --configfile " + configFile
    return cmd


if __name__ == "__main__":

    usage = """%prog [option]"""

    description = """This script will run forcedPhotCoadd for a given list of filters and patches. 
    The  default if to use f.list files (where 'f' is a filter in ugriz) and patches_f.txt, 
    and launch forcedPhotCoadd in several batch jobs. You thus need to be running it at CC-IN2P3 
    to make it work. To run all  filters, you can do something like 
    %prog -f ugriz -m 1 -c YOURCONFIGFILE -a"""

    opts, args = LR.standard_options(usage=usage, description=description)

    opts.mod = 1
    opts.input = "pardir/output"
    opts.output = "pardir/output"

    for filt in opts.filters:
        filePatch = "patches_" + filt + ".txt"

        cmd = "split -l " + str(opts.mod) + " -d " + filePatch + " " + filePatch + "_"
        os.system(cmd)

        patchList = glob.glob(filePatch + "_*")
        print("\nWorning on filter %s: %i patches (jobs)" % (filt, len(patchList)))
        for patch in sorted(patchList):
            cmd = build_cmd(patch, opts.configs, filt, opts.input, opts.output)
            LR.submit(cmd, patch, filt, autosubmit=opts.autosubmit, from_slac=opts.fromslac)
