#!/usr/bin/env python

"""
.. _createPatchLists:

Build the list of patches
=========================
"""


from __future__ import print_function
import os
import subprocess
import glob
import numpy as np
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

    # Check the input filter
    files = glob.glob("*_patches.list")

    all_tracts_patches = np.concatenate([np.loadtxt(f, dtype='bytes').astype(str) for f in files])
    all_tracts_patches = set(np.concatenate(all_tracts_patches))
    np.savetxt("patches.txt", all_tracts_patches, fmt="%s")
    for filt in opts.filters:
        files = glob.glob("%s_*_patches.list" % filt)
        all_tp = np.concatenate([np.loadtxt(f, dtype='bytes').astype(str) for f in files])
        all_tp = set(np.concatenate(all_tp))
        all_tp = [("--id filter=%s " % filt + tp) for tp in all_tp]
        np.savetxt("patches_%s.txt" % filt, all_tp, fmt="%s")

    cmd = "sed -e 's/^/--id filter=%s /' patches.txt > patches_all.txt" % ("^".join(opts.filters))
    print("\nRUNNING:", cmd)
    subprocess.call(cmd, shell=True)
    print("INFO: End of run")
