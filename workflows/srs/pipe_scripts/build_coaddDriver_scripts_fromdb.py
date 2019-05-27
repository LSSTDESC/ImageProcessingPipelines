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


__author__ = 'Johann Cohen-Tanugi <johann.cohen-tanugi@umontpellier.fr>'
__version__ = '$Revision: 1.0 $'


if __name__ == "__main__":

    usage = """%prog input [option]"""
    description = """Report tracts and patches continaing images"""

    parser = OptionParser(description=description, usage=usage)
    parser.add_option("-f", "--filt", type="string",
                      help="A filter name", default=None)
    parser.add_option("-o", "--outdir", type="string",
                      help="scripts output directory", default=None)
    parser.add_option("-t", "--tractfile", type="string",
                      help="tract/patch file", default=None)
    opts, args = parser.parse_args()

    for filt in opts.filt:
        dirout = opts.outdir
        os.system("mkdir -p %s/%s"%(dirout,filt))
        db = sqlite3.connect(args[0])
        cursor = db.cursor()
        if opts.tractfile is None:
            cursor.execute("SELECT DISTINCT tract FROM overlaps where filter='%s'"%filt)
            tract_list=np.array(cursor.fetchall()).flatten()
        else:
            tract_list = np.genfromtxt(opts.tractfile, dtype=str)

        for i,tract in enumerate(tract_list):
            if len(tract)==2:
                tract,patches = tract_list[i]
                id_string = '--id tract=%s patch=%s filter=%s'%(tract,patches,filt)
                patches='('+patches.replace(',',', ')+')'
                patch_str="patch=\'%s\'"%patches
                if '^' in patches:
                    patch_str=patch_str.replace('^',')\' or patch=\'(')
                    patch_str='('+patch_str+')'
                cursor.execute("SELECT DISTINCT visit,detector FROM overlaps WHERE tract=%s and %s and filter='%s'"%(tract,patch_str,filt))
            else:
                tract=str(tract)
                id_string = '--id tract=%s filter=%s'%(tract,filt) 
                cursor.execute("SELECT DISTINCT visit,detector FROM overlaps WHERE tract=%s and filter='%s'"%(str(tract),filt))
            d=cursor.fetchall()
            dd=np.array(d,dtype=str)
            ss=np.repeat('--selectId visit=',len(dd))
            ss2=np.repeat(' detector=',len(dd))
            dummy=np.core.defchararray.add(ss, dd[:,0])
            dummy2=np.core.defchararray.add(dummy, ss2)
            final=np.core.defchararray.add(dummy2, dd[:,1])
            filename2 = '%s/%s/%s_visits.list'%(dirout,filt,tract)
            np.savetxt(filename2,final, fmt='%s')

            filename = '%s/%s/tract_%s.sh'%(dirout,filt,tract)
            to_write = ["#!/bin/bash\nDM_SETUP=%s\nsource ${SETUP_LOCATION}/DMsetup.sh\nexport OMP_NUM_THREADS=1"%(os.environ['DM_SETUP'])]
            if filt=='u':
                to_write.extend(['coaddDriver.py %s --rerun %s %s @%s --cores $((NSLOTS+1)) --doraise --configfile=${OBS_LSST_DIR}/config/coaddDriver_noPSF.py --longlog -c makeCoaddTempExp.doApplySkyCorr=True --loglevel CameraMapper=warn'%(os.environ['IN_DIR'],os.environ['RERUN'],id_string,filename2)])
            else:
                to_write.extend(['coaddDriver.py %s --rerun %s %s @%s --cores $((NSLOTS+1)) --doraise --longlog -c makeCoaddTempExp.doApplySkyCorr=True --loglevel CameraMapper=warn'%(os.environ['IN_DIR'],os.environ['RERUN'],id_string,\
filename2)])
            np.savetxt(filename, to_write, fmt="%s")
            os.system("chmod a+x %s"%filename)
