#!/usr/bin/env python

"""
.. _build_coaddDriver_scripts_fromdb:

Build the coadd scripts and the list of visits in tracts for a given filter
===========================================
"""


from __future__ import print_function
import os
from optparse import OptionParser
import sqlite3
import numpy as np


__author__ = 'Nicolas Chotard <johann.cohen-tanugi@umontpellier.fr>'
__version__ = '$Revision: 1.0 $'


if __name__ == "__main__":

    usage = """%prog input [option]"""
    description = """Report tracts and patches continaing images"""

    parser = OptionParser(description=description, usage=usage)
    parser.add_option("-f", "--filt", type="string",
                      help="A filter name", default=None)
    opts, args = parser.parse_args()

    for filt in opts.filt:
        dirout=os.path.join(os.environ['OUT_DIR'],"rerun",os.environ['RERUN2'],"scripts")
        db = sqlite3.connect(args[0])
        cursor = db.cursor()
        cursor.execute("SELECT DISTINCT tract FROM overlaps where filter='%s'"%filt)
        tract_list=np.array(cursor.fetchall()).flatten()
        tract_list=np.array(5063).flatten()
        for tract in tract_list:
            os.system("mkdir -p %s/%s"%(dirout,filt))
            id_string = '--id tract=%i filter=%s'%(tract,filt)
            cursor.execute("SELECT DISTINCT visit,detector FROM overlaps WHERE tract=%i and filter='%s'"%(tract,filt))
            d=cursor.fetchall()
            dd=np.array(d,dtype=str)
            ss=np.repeat('--selectId visit=',len(dd))
            ss2=np.repeat(' detector=',len(dd))
            dummy=np.core.defchararray.add(ss, dd[:,0])
            dummy2=np.core.defchararray.add(dummy, ss2)
            final=np.core.defchararray.add(dummy2, dd[:,1])
            filename2 = '%s/%s/%i_visits.list'%(dirout,filt,tract)
            np.savetxt(filename2,final, fmt='%s')

            filename = '%s/%s/tract_%i.sh'%(dirout,filt,tract)
            to_write = ["#!/bin/bash\nDM_SETUP=%s\nsource ${SETUP_LOCATION}/DMsetup.sh\nexport OMP_NUM_THREADS=1"%(os.environ['DM_SETUP'])]
            if filt=='u':
                to_write.extend(['coaddDriver.py %s --rerun %s %s @%s --cores ${NSLOTS} --doraise --configfile=${OBS_LSST_DIR}/config/coaddDriver_TimeBestSeeing.py --longlog'%(os.environ['IN_DIR'],os.environ['RERUN'],id_string,filename2)])
            else:
                to_write.extend(['coaddDriver.py %s --rerun %s %s @%s --cores ${NSLOTS} --doraise --configfile=${OBS_LSST_DIR}/config/coaddDriver_TimeBestSeeing.py --longlog'%(os.environ['IN_DIR'],os.environ['RERUN'],id_string,filename2)])
            np.savetxt(filename, to_write, fmt="%s")
            os.system("chmod a+x %s"%filename)
