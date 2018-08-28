#!/usr/bin/env python3

import os
import lsst.daf.persistence as dafPersist
import lsst.afw.display.ds9 as ds9
from astropy.table import Table
import numpy as np

filt = 'g'
print('Using filter ', filt)

path = "/sps/lsst/users/lsstprod/desc/DC2-test/newCam/input/rerun/boutigny/calib"
calDir = os.path.join(path, 'calexp')

dir = os.listdir(calDir)
visits = []
for d in dir:
    if d[-1:] == filt:
        visits.append(int(d[:-2]))

print('Found ', len(visits), ' visits')

butler = dafPersist.Butler(path)

f = {}
e = {}
did = {}
for visit in visits:
    # print(visit)
    for count,data_ref in enumerate(butler.subset('src', visit=visit)):
        if data_ref.datasetExists():
            dataId = data_ref.dataId
        else:
            continue

        calib = butler.get("calexp_calib", dataId)
        flux0, fluxErr0 = calib.getFluxMag0()
        raft = dataId['raftName']
        ccd = dataId['detectorName']
        if (raft, ccd) not in f.keys():
            f[(raft, ccd)]=[]
            e[(raft, ccd)]=[]
            # we need to record a valid dataId corresponding to this(raft, ccd) 
            did[(raft, ccd)] = dataId
        f[(raft, ccd)].append(flux0)
        e[(raft, ccd)].append(fluxErr0)

fav = {}
for k in f.keys():
#    fav[k] = np.average(np.asarray(f[k]), weights=1.0/np.asarray(e[k]))
    fav[k] = np.median(np.asarray(f[k]))    

weight = {}
for k in f.keys():
    weight[k] = fav[k] / fav[('R22', 'S11')]

for k in weight.keys():
    dataId = did[k]
    fn = butler.get('flat_filename', dataId)[0]
    w = weight[k]
    flat = butler.get('flat', dataId)
    flat.image *= w

    print('Writing : ', fn)
    flat.writeFits(fn)

