#!/usr/bin/env python3

import os
import time

def sensor():
    s = []
    for i in range(3):
        for j in range(3):
            s.append('S' + str(i) + str(j))
    return s;

def raft():
    r = []
    for i in range(5):
        for j in range(5):
            if (i == 0 and j == 0) or (i == 4 and j == 0) or (i == 0 and j == 4) or (i == 4 and j == 4):
                continue
            r.append('R' + str(i) + str(j))
    return r;

def writeScripts(rafts, sensors, filt):
    files = []
    for r in rafts:
        cmd = 'qsub -P P_lsst_prod -q long -l sps=1 -j y -o /sps/lsst/users/lsstprod/desc/DC2-test/newCam/util/log/'
        cmd += filt + '/' + str(r) +'_' + filt + '.log <<EOF \n'
        cmd += ' cd /sps/lsst/users/lsstprod/desc/DC2-test/newCam \n'
        cmd += ' source setup.sh \n'
        file = r + '_' + filt + '.sh'
        for s in sensors:
            id = '--id filter=' + filt + ' raftName=' + r + ' detectorName=' + s
            cmd += ' constructFlat.py input --rerun boutigny/calib --batch-type none ' + id + ' \n'
        cmd += 'EOF'
        fn = open(file, 'w')
        fn.write(cmd)
        fn.close()
        files.append(file)
        os.chmod(file, 0o755)
        os.system("./"+file)
        time.sleep(1)
    return files

def main():
    rafts = raft()
    sensors = sensor()
    filt = 'y'
    files = writeScripts(rafts, sensors, filt)

if __name__ == "__main__":
    main()
