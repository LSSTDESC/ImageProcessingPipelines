#!/usr/bin/env python3

import os
from os import listdir
from os.path import isdir, join
import time

def writeScripts(visit, filt):
    cmd = 'qsub -P P_lsst_prod -q mc_long -pe multicores 8 -l sps=1 -j y -o /sps/lsst/users/lsstprod/desc/DC2-test/newCam/util/log/'
    cmd += filt + '/' + str(visit) + '.log <<EOF \n'
    cmd += ' cd /sps/lsst/users/lsstprod/desc/DC2-test/newCam \n'
    cmd += ' source setup.sh \n'
    file = str(visit) + '.sh'
    cmd += ' singleFrameDriver.py input --rerun boutigny/calib --id visit=' + str(visit) + ' --cores 8 \n'
    cmd += 'EOF\n'
    fn = open(file, 'w')
    fn.write(cmd)
    fn.close()
    os.chmod(file, 0o755)
    os.system("./"+file)
    time.sleep(1)
    return file

def main():
    filt = 'i'
    dir = '/sps/lsst/users/lsstprod/desc/DC2-test/rawDownload/new/DC2-R1-2p-WFD-' + filt
    dirs = [f for f in listdir(dir) if isdir(join(dir, f))]
    for d in dirs:
        visit = int(d[:-2])
        f = writeScripts(visit, filt)
#        print(f)

if __name__ == "__main__":
    main()
