#!/usr/bin/env python

"""
.. _build_patch_lists:

Build the list of patches
=========================
"""


from __future__ import print_function
import os
import subprocess
from optparse import OptionParser


__author__ = 'Nicolas Chotard <nchotard@in2p3.fr>'
__version__ = '$Revision: 1.0 $'


if __name__ == "__main__":

    usage = """%prog [option]"""
    description = """This script will find the patches for all filters"""

    parser = OptionParser(description=description, usage=usage)
    parser.add_option("-f", "--filters", type="string",
                      help="Filter(s) [%default]. Can also be a ist of filter ('ugriz')")
    parser.add_option("-c", "--config", type="string", default="makeSkyMapConfig.py",
                      help="If not given or present in the local dir, a standard one will be created.")
    parser.add_option("--input", type="string", default='pardir/output',
                      help='input directory [%default]')
    parser.add_option("--output", type="string", default='pardir/output',
                      help='output directory [%default]')
    opts, args = parser.parse_args()

    if not os.path.exists(opts.config):
        raise "WARNING: The given (or default) configuration file does not exists."

    opts.filters = [filt for filt in opts.filters.split(",") if os.path.exists('%s.list' % filt)]

    # Create a file containing the list of all visits
    cmd = "cat [%s].list > all.list" % "\|".join(opts.filters)
    os.system(cmd)

    print("INFO: Running all commands for all visits")
    # makeSkyMap command
    cmd = "makeSkyMap.py %s --output %s --configfile %s" % \
          (opts.input, opts.output, opts.config)
    print("RUNNING:", cmd)
    subprocess.call(cmd, shell=True)

    cmd = 'reportPatchesWithImages.py %s --visits all.list | grep "^tract" > patches.txt' % \
          (opts.output)
    print("RUNNING:", cmd)
    subprocess.call(cmd, shell=True)

    # Check the input filter
    for filt in opts.filters:
        if not os.path.exists('%s.list' % filt):
            print("WARNING: No data for filter", filt)
            continue
        cmd = "sed -e 's/^/--id filter=%s /' patches.txt > patches_%s.txt" % (filt, filt)
        print("\nRUNNING:", cmd)
        subprocess.call(cmd, shell=True)

    cmd = "sed -e 's/^/--id filter=%s /' patches.txt > patches_all.txt" % ("^".join(opts.filters))
    print("\nRUNNING:", cmd)
    subprocess.call(cmd, shell=True)
    print("INFO: End of run")
