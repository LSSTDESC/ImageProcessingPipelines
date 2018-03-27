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
from lsst.afw.coord import Fk5Coord
import lsst.daf.persistence as dafPersist
import numpy as np


def organize_by_visit(dataids, visits=None):
    dataids_visit = {}
    for dataid in dataids:
        if visits is not None and str(dataid['visit']) not in visits:
            continue
        if dataid['visit'] not in dataids_visit:
            dataids_visit[dataid['visit']] = []
        dataids_visit[dataid['visit']].append(dataid)
    return dataids_visit


def get_visit_corners(butler, dataids, ccds=None, getccds=False):
    ras, decs, accds = [], [], []
    for dataid in dataids:
        if ccds is not None and dataid['ccd'] not in ccds:
            continue
        calexp = butler.get('calexp', dataId=dataid)
        coords = [calexp.getWcs().pixelToSky(point)
                  for point in geom.Box2D(calexp.getBBox()).getCorners()]
        ras.extend([coord.getRa().asDegrees() for coord in coords])
        decs.extend([coord.getDec().asDegrees() for coord in coords])
        accds.extend([dataid['ccd']] * 4)
    tocoords = lambda ra, dec: Fk5Coord(geom.Point2D(ra, dec), geom.degrees)
    if not getccds:
        return [tocoords(min(ras), min(decs)), tocoords(min(ras), max(decs)),
                tocoords(max(ras), max(decs)), tocoords(max(ras), min(decs))]
    else:
        return [accds[np.argmin(ras)], accds[np.argmin(decs)],
                accds[np.argmax(ras)], accds[np.argmax(decs)]]

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


def reportPatchesWithImages(butler, visits=None):

    # create a butler object associated to the output directory
    butler = dafPersist.Butler(butler)

    # Get the skymap
    skyMap = butler.get("deepCoadd_skyMap")

    # Get the calexp metadata
    keys = sorted(butler.getKeys("calexp").keys())
    metadata = butler.queryMetadata("calexp", format=keys)

    # Create a list of available dataids
    dataids = [dict(zip(keys, list(v) if not isinstance(v, list) else v)) for v in metadata]

    # Organize the dataids by visit
    vdataids = organize_by_visit(dataids, visits=visits)

    # Get the ccds that will be used to compute the visit corner coordinates
    # this depend on the instrument, so cannot be hardcoded
    ccds = get_visit_corners(butler, vdataids[list(vdataids)[0]], getccds=True)

    # Get the corners coordinates for all visits
    allcoords = [get_visit_corners(butler, vdataids[vdataid], ccds=ccds) for vdataid in vdataids]

    # Get the tract/patch list in which the visits are
    alltps = []
    for vdataid, vcoords in zip(vdataids, allcoords):
        alltps.extend(get_tps(skyMap, vcoords, vdataids[vdataid][0]['filter']))

    # Re-organize the tract and patch list into a dictionnary
    tps = {}
    for tp in alltps:
        if tp[0] not in tps:
            tps[tp[0]] = []
        if tp[1] not in tps[tp[0]]:
            tps[tp[0]].append(tp[1])

    return tps


__author__ = 'Nicolas Chotard <nchotard@in2p3.fr>'
__version__ = '$Revision: 1.0 $'


if __name__ == "__main__":

    usage = """%prog input [option]"""
    description = """Report tracts and patches continaing images"""

    parser = OptionParser(description=description, usage=usage)
    parser.add_option("-v", "--visits", type="string",
                      help="Optional list of visits (file or coma separated list)")
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

    # Get the full list of tract/patch in which are all visits
    tps = reportPatchesWithImages(args[0], visits=opts.visits)

    # Print the result
    for tract in tps:
        for patch in tps[tract]:
            print("tract=%i patch=%i,%i" % (tract, patch[0], patch[1]))
