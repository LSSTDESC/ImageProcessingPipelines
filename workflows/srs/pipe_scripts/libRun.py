#!/usr/bin/env python

"""
.. _libRun:

Tools to run cluster analysis
=============================
"""


from __future__ import print_function
import os
import re
import time
from optparse import OptionParser
import numpy as np


__author__ = 'Nicolas Chotard <nchotard@in2p3.fr>'
__version__ = '$Revision: 1.0 $'


def makeFileName(patchList) :
    s = re.split('--', patchList)
    name = ""
    for i in range(1, len(s)) :
        field = []
        t = re.split(" ", s[i])
        for j in range(1, 4):
            field.append(re.split("=", t[j])[1])
        if i > 1:
            name = name + "_"
        name = name + field[0] + "_" + field[1] + "_" + field[2]
    return name.replace(", ", "-")

def submit(cmd, prefix, filt=None, autosubmit=False, ct=60000, vmem='4G',
           system=None, queue=None, otheroptions=None, from_slac=False,
           from_nersc=False):
    """
    cmd: command line to run
    prefix: used for the .log and .sh file names
    filt: name of the current filter
    autosubmit: if True, the job will be created and submited automatically
    """
    script_path = "scripts" + (("/" + filt) if filt is not None else "")
    if not os.path.isdir(script_path):
        os.makedirs(script_path)
    cwd = os.getcwd()
    dirLog = cwd + "/log" + (("/" + filt) if filt is not None else "")
    if not os.path.isdir(dirLog):
        os.makedirs(dirLog)
    log = dirLog + "/" + prefix + ".log"
    print("LOG:", log)
    options = "sps=1"
    if ct is not None:
        options += ",ct=%i" % ct
    if vmem is not None:
        options += ",h_vmem=%s" % vmem
    if system is not None:
        options += ",os=" + system

    qsub = "qsub -P P_lsst -l %s -j y -o " % options + log
    if queue is not None:
        qsub += " -q " + queue
    if otheroptions is not None:
        qsub += " %s" % otheroptions 
    qsub += " <<EOF"
    scriptname = script_path + "/" + prefix + ".sh"
    script = open(scriptname, "w")
    if from_slac:
        script.write("#!/usr/local/bin/bash\n")
        script.write("#$ -P P_lsst" + "\n")
        for opt in options.split(","):
            script.write("#$ -l %s\n" % opt)
        if queue is not None:
            script.write("#$ -q %s\n" % queue)
        if otheroptions is not None:
            script.write("#$ %s\n" % otheroptions)
        script.write("#$ -j y\n")
        script.write("#$ -o %s\n" % log)
        if "singleFrameDriver.py" in cmd:
            script.write("export OMP_NUM_THREADS=1\n")
        script.write("source ${SETUP_LOCATION}/DMsetup.sh\n")
        script.write("cd " + cwd + "\n")
        script.write(cmd + "\n")
    elif from_nersc:
        script.write("#!/bin/bash\n")
        script.write("source ${SETUP_LOCATION}/DMsetup.sh\n")
        script.write("cd " + cwd + "\n")
        script.write(cmd + "\n")
    else:
        script.write(qsub + "\n")
        script.write("#!/usr/local/bin/bash\n")
        if "singleFrameDriver.py" in cmd:
            script.write("export OMP_NUM_THREADS=1\n")
        script.write(" cd " + cwd + "\n")
        script.write(" source pardir/setup.sh\n")
        script.write(" " + cmd + "\n")
        script.write("EOF" + "\n")

    script.close()
    os.system("chmod +x " + scriptname)
    print("SCRIPT:", cwd + "/" + scriptname)
    if autosubmit and not from_slac and not from_nersc:
        os.system("./"+scriptname)
        time.sleep(0.2)

def job_number(items, max_item, max_job):
    """
    How many jobs should we be running (and how many items in each?)?
    """
    assert max_item > 0, "max_item must be > 0"
    assert max_job > 1, "max_job must be > 0"
    njobs = int(np.ceil(float(len(items))/max_item))
    if njobs > max_job:
        print("WARNING: number of jobs exceed the maximum. More items will be put in each job.")
        njobs = max_job
    return njobs

def organize_items(items, njobs):
    items = [a.tolist() for a in np.array_split(sorted(items), njobs)]
    print("INFO: Items sub-divided as followed in %i jobs:" % njobs)
    for i, it in enumerate(items):
        if i == 5:
            print(" - ...")
        elif i > 5:
            continue
        else:
            print(" -", "_".join(np.array(it, dtype='str')))
    return items

def select_config(configs, filt):
    if configs is None:
        return configs
    if "_" + filt + ".py" in configs:                
        config = [c for c in configs.split(',') if "_"+filt in c][0]
    else:
        config = configs.split(',')[0]  # default configuration file must be the first of the list  
    return config

def standard_options(usage=None, description=None):

    parser = OptionParser(description=description, usage=usage)
    parser.add_option("-f", "--filters", type="string", default=None,
                      help="Filter(s) [%default]. Can also be a list of filter (comma separated)")
    parser.add_option("-c", "--configs", type="string", default=None,
                      help="Configuration file [%default]. Several files (and filters with option -f) "
                      "can be given if a filter needs its own config file. In that case, include"
                      " '_f' in the name of the file. e.g.: processConfig.py,processConfig_u.py if you"
                      " want the 'u' filter to use a different configuration file. The default config"
                      " must be the first one.")
    parser.add_option("-i", "--input", type="string", default="pardir/input", help="Input directory")
    parser.add_option("-o", "--output", type="string", default="pardir/output",
                      help="output directory")
    parser.add_option("-m", "--mod", type="int", default=4,
                      help="Nbr. of visits per job [%default]")
    parser.add_option("-M", "--max", type="int", default=999,
                      help="Max nbr of jobs to be submitted [%default]")
    parser.add_option("-a", "--autosubmit", action='store_true', default=False,
                      help="Submit the jobs automatically")
    parser.add_option("-s", "--system", type="string", default="cl7",
                      help="System used to run the jobs")
    parser.add_option("--vmem", type="string", default='4G', help="Job memory [%default]")
    parser.add_option("--ct", type="int", default='60000', help="Job cpu time [%default]")
    parser.add_option("--queue", type="string", help="Job queue [%default]")
    parser.add_option("--otheroptions", type="string", help="Other options [%default]")
    parser.add_option("--multicore", action='store_true', default=False,
                      help="Multicore jobs (mostly for processCcd)")
    parser.add_option("--doraise", action='store_true', default=False,
                      help="doraise")
    parser.add_option("--time", action='store_true', default=False,
                      help="time the commands executed")
    parser.add_option("--fromslac", action='store_true', default=False,
                      help="Run job from slac workflow interface")
    parser.add_option("--perraft", action='store_true', default=False,
                      help="One raft per job, instead of one entire visit")
    parser.add_option("--fromnersc", action='store_true', default=False,
                      help="Run job from slac workflow interface at NERSC")
    opts, args = parser.parse_args()

    if opts.filters is None:
        raise IOError("No input filters")
    opts.filters = opts.filters.split(",")
    print("INFO: We will run of the following filter(s)", opts.filters)

    return opts, args

