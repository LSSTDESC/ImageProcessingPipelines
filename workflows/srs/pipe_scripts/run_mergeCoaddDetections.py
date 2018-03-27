#!/usr/bin/env python


from __future__ import print_function
import os
import glob
import libRun as LR


def build_cmd(patch, configFile, input, output):

    if not os.path.isdir("scripts"):
        os.makedirs("scripts")

    cmd = "mv "+ patch + " scripts"
    os.system(cmd)

    cmd = "mergeCoaddDetections.py %s --output %s"  % (input, output) + \
          " @scripts/" + patch + " --configfile " + configFile

    return cmd


if __name__ == "__main__":

    usage = """%prog [option]"""

    description = """This script will run mergeCoaddDetections for a given list of filters and patches. 
    The  default if to use f.list files (where 'f' is a filter in ugriz) and patches_f.txt, 
    and launch detectCoadSources in several batch jobs. You thus need to be running it at CC-IN2P3 
    to make it work. To run all  filters, you can do something like 
    %prog -f ugriz -m 1 -c detectCoaddSources.py -a"""

    opts, args = LR.standard_options(usage=usage, description=description)

    # overwrite some options
    opts.mod = 2
    opts.input = "pardir/output"
    opts.output = "pardir/output"
    file_patch = "patches_all.txt"

    cmd = "split -l " + str(opts.mod) + " -d " + file_patch + " " + file_patch + "_"
    os.system(cmd)
    patch_list = sorted(glob.glob(file_patch + "_*"))

    for patch in sorted(patch_list):
        print("\n", patch)
        # Only submit the job if asked
        cmd = build_cmd(patch, opts.configs, opts.input, opts.output)
        LR.submit(cmd, patch, autosubmit=opts.autosubmit, ct=10000, vmem='4G',
                  from_slac=opts.fromslac)
