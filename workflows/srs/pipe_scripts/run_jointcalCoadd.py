#!/usr/bin/env python


from __future__ import print_function
import numpy as N
import libRun as LR
import os


def build_cmd(patches, config, filt, input, output):
    cmd = "jointcalCoadd.py %s --output %s " % (input, output) + patches + " @" + \
          filt + ".list" + " --configfile " + config + " --clobber-config"
    print("\nCMD:", cmd)
    return cmd

if __name__ == "__main__":

    usage = """%prog [option]"""

    description = """Run jointcalCoadd.py for a given list of filters and patches."""

    opts, args = LR.standard_options(usage=usage, description=description)

    # overwrite the -m option to force it to be 5
    opts.mod = 5

    # Loop over filters
    for filt in opts.filters:

        config = LR.select_config(opts.configs, filt)

        # Are there visits to load
        if not os.path.exists("patches_" + filt + ".txt"):
            print("WARNING: No file (no visit) for filter", filt)
            continue

        # default options
        opts.input = "pardir/output"
        opts.output = "pardir/output"

        # Get the list of patches
        patches = [" ".join(p) for p in
                   N.loadtxt("patches_" + filt + ".txt", dtype='bytes').astype(str)]
        print("INFO: %i patches loaded: " % len(patches), "e.g.", patches[0])

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
            prefix = "patches_%03d" % (i + 1)
            LR.submit(cmd, prefix, filt, autosubmit=opts.autosubmit, queue=opts.queue,
                      ct=6000, vmem=opts.vmem, system=opts.system, from_slac=opts.fromslac)

            N.savetxt("scripts/%s/patches_%03d.list" % (filt, i + 1),
                      N.array(ps, dtype='str').tolist(), fmt="%s")

    if not opts.autosubmit:
        print("\nINFO: Use option --autosubmit to submit the jobs")

