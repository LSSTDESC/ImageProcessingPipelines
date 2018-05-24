#!/usr/bin/env python

"""
.. _runMakeSkyMap:

Build the sky map
=================
"""


from __future__ import print_function
import os
import subprocess
from optparse import OptionParser


__author__ = 'Nicolas Chotard <nchotard@in2p3.fr>'
__version__ = '$Revision: 1.0 $'


if __name__ == "__main__":

    usage = """%prog [option]"""
    description = """This script will run makeSkyMap.py"""

    parser = OptionParser(description=description, usage=usage)
    parser.add_option("-f", "--filters", type="string",
                      help="Filter(s) [%default]. Can also be a ist of filter ('ugriz')")
    parser.add_option("-c", "--config", type="string", default="makeSkyMapConfig.py",
                      help="If not given or present in the local dir, a standard one will be created.")
    parser.add_option("-i", "--input", type="string", default='pardir/output',
                      help='input directory [%default]')
    parser.add_option("-o", "--output", type="string", default='pardir/output',
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