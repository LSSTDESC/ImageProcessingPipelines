#!/usr/bin/env python3

import os
from os import listdir
from os.path import isdir, join
import time

def writeScripts(filt):
    cmd = 'qsub -P P_lsst_prod -q mc_long -pe multicores 8 -l sps=1 -j y -o /sps/lsst/users/lsstprod/desc/DC2-test/newCam/log/ingest_'
    cmd += filt + '.log <<EOF \n'
    cmd += ' cd /sps/lsst/users/lsstprod/desc/DC2-test/newCam \n'
    cmd += ' source setup.sh \n'
    file = 'ingest_'+ filt + '.sh'
    cmd += ' ingestDriver.py input @' + filt + '.list --cores 8 \n'
    cmd += 'EOF\n'
    fn = open(file, 'w')
    fn.write(cmd)
    fn.close()
    os.chmod(file, 0o755)
#    os.system("./"+file)
#    time.sleep(1)
    return file

def main():
    for filt in ['r', 'i', 'z', 'y']:
        f = writeScripts(filt)
#        print(f)

if __name__ == "__main__":
    main()
