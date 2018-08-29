#!/usr/bin/env python

"""
.. _reportPatchesWithImages:

Report tracts and patches continaing images
===========================================
"""


from __future__ import print_function
import os
from optparse import OptionParser
import lsst.afw.geom as geom
try:  
    from lsst.afw.coord import Fk5Coord
    tocoords = lambda ra, dec: Fk5Coord(geom.Point2D(ra, dec), geom.degrees)
except:  # > w_2018_11
    tocoords = lambda ra, dec: geom.SpherePoint(ra, dec, geom.degrees)
import lsst.daf.persistence as dafPersist
import numpy as np


def organize_by_visit(metadata, keys, visits=None):
    if visits is None:
        # Turn the list of tuples into a dictionary
        dataids = [dict(zip(keys, list(v) if not isinstance(v, list) else v)) for v in metadata]
    else:
        dataids = [dict(zip(keys, list(v) if not isinstance(v, list) else v)) for v in metadata if str(v[4]) in visits]
    return dataids



def get_visit_corners(butler, dataids, ccds=None, getccds=False, ccdkey='sensor'):
    ras, decs, accds = [], [], []
    for ii, dataid in enumerate(dataids):
        if ccds is not None and dataid[ccdkey] not in ccds:
            continue
        calexp_bbox = butler.get('calexp_bbox', dataId=dataid)
        calexp_wcs = butler.get('calexp_wcs', dataId=dataid)
        coords = [calexp_wcs.pixelToSky(point)
                  for point in geom.Box2D(calexp_bbox).getCorners()]
        ras.extend([coord.getRa().asDegrees() for coord in coords])
        decs.extend([coord.getDec().asDegrees() for coord in coords])
        accds.extend([dataid[ccdkey]] * 4)
    if not getccds:
        return [tocoords(min(ras), min(decs)), tocoords(min(ras), max(decs)),
                tocoords(max(ras), max(decs)), tocoords(max(ras), min(decs))]
    else:
        return [accds[np.argmin(ras)], accds[np.argmin(decs)],
                accds[np.argmax(ras)], accds[np.argmax(decs)]]


def get_dataid_corners(butler, dataids, ccdkey='sensor'):
    coords = []
    for ii, dataid in enumerate(dataids):
        print("Runing on dataId %i / %i :" % (ii + 1, len(dataids)), dataid)
        calexp = butler.get('calexp', dataId=dataid)
        coords = [calexp.getWcs().pixelToSky(point)
                  for point in geom.Box2D(calexp.getBBox()).getCorners()]
    return coords


def get_tps(skymap, coords, filt=None):
    tplist = skymap.findTractPatchList(coords)
    tps = []
    for tp in tplist:
        tract = tp[0].getId()
        for patch in tp[1]:
            if filt is not None:
                tps.append((tract, patch.getIndex(), filt))
            else:
                tps.append((tract, patch.getIndex()))
    return sorted(list(set(tps)))


def reportPatchesWithImages(butler, visits=None, ccdkey='sensor', filt=None):

    # create a butler object associated to the output directory
    butler = dafPersist.Butler(butler)

    # Get the skymap
    skyMap = butler.get("deepCoadd_skyMap")

    # Get the calexp metadata
    keys = sorted(butler.getKeys("calexp").keys())
    if filt is None:
        metadata = butler.queryMetadata("calexp", format=keys)
    else:
        metadata = butler.queryMetadata("calexp", format=keys, dataId={'filter':filt})
    metadata = butler.queryMetadata("calexp", format=keys)

    # Organize the dataids by visit
    vdataids = organize_by_visit(dataids, visits=visits)

    # Get the ccds that will be used to compute the visit corner coordinates
    # this depend on the instrument, so cannot be hardcoded
    #in the case of lsst this just returns ['S20', 'S22', 'S02', 'S02']
    ccds = ['S20', 'S22', 'S00', 'S02']
    # Get the corners coordinates for all visits
    # This is very inefficient in theory as we know which sensors are at the focla plane's boundary.
    # In practice unfortunately for Run1.2p we are not guaranteed that all these boundary sensors are simulated
    # so we are forced to loop over all the visit's sensors, which is a major drag.
    allcoords = []
    for ii, vdataid in enumerate(vdataids):
        print("Running on visit %i (%03d / %i)" % (vdataid,ii + 1, len(vdataids)))
        allcoords.append(get_visit_corners(butler, vdataids[vdataid], ccds=ccds, ccdkey=ccdkey))
    # Get the tract/patch list in which the visits are
    for vdataid, vcoords in zip(vdataids, allcoords):
        visit = vdataid
        filt = vdataids[vdataid][0]['filter']
        filter_tracts = tps.get(filt,{})
        alltps = get_tps(skyMap, vcoords)
        for tinfo in alltps:
            tract = tinfo[0] #discard patches as they are not useful for coaddDriver execution
            if tract in filter_tracts:
                if visit not in filter_tracts[tract]:
                    filter_tracts[tract].append(visit)
            else:
                filter_tracts[tract] = [visit]
        tps[filt] = filter_tracts


    return tps


__author__ = 'Nicolas Chotard <nchotard@in2p3.fr>'
__version__ = '$Revision: 1.0 $'


if __name__ == "__main__":

    usage = """%prog input [option]"""
    description = """Report tracts and patches continaing images"""

    parser = OptionParser(description=description, usage=usage)
    parser.add_option("-v", "--visits", type="string",
                      help="Optional list of visits (file or coma separated list)")
    parser.add_option("--ccdkey", type="string",
                      help="CCD key", default='sensor')
    parser.add_option("-f", "--filt", type="string",
                      help="A filter name", default=None)
    opts, args = parser.parse_args()

    # Is there a list of visit given by the use?
    if opts.visits is not None:
        if os.path.exists(opts.visits):
            opts.visits = np.loadtxt(opts.visits, dtype='str', unpack=True)
            if len(opts.visits) != 1:
                opts.visits = [vis.split('=')[1]
                               for vis in opts.visits[['visit' in arr[0]
                                                       for arr in opts.visits]][0]]
        else:
            opts.visits = opts.visits.split(',')
        print("%s visit requested" % len(opts.visits))

    # Get the full list of tract/patch in which are all visits
    tps = reportPatchesWithImages(args[0], visits=opts.visits, ccdkey=opts.ccdkey, filt=opts.filt)

    tract_list = []
    for filt in tps:
        os.system('mkdir -p scripts/%s'%filt)
        tract_dict = tps[filt]
        tract_list.extend(tract_dict.keys())
        for tract in tract_dict:
            filename = 'scripts/%s/tract_%i.sh'%(filt,tract)
            filename2 = '%s/03-coadd/scripts/%s/%i_visits.list'%(os.environ['WORK_DIR'],filt,tract)

            visit_list = tract_dict[tract]
            np.savetxt(filename2, ['--selectId visit=%s'%v for v in visit_list], fmt="%s")

            to_write = ["#!/bin/bash\nDM_SETUP=%s\nsource ${SETUP_LOCATION}/DMsetup.sh\nexport OMP_NUM_THREADS=1"%(os.environ['DM_SETUP'])]
            to_write.extend(['coaddDriver.py  %s --rerun %s --id tract=%i filter=%s @%s --cores ${NSLOTS} --doraise'%(os.environ['IN_DIR'],os.environ['RERUN'],tract, filt, filename2)])
            np.savetxt(filename, to_write, fmt="%s")
            os.system("chmod a+x %s"%filename)
    np.savetxt('scripts/tracts.list',tract_list, fmt="%s")
