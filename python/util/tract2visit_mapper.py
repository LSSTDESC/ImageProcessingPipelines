#!/usr/bin/env python

from __future__ import print_function, division, absolute_import

import sqlite3
#import argparse
import lsst.sphgeom
from lsst.afw import geom
from lsst.daf.persistence import Butler
import numpy as np
import os
from optparse import OptionParser
import pickle

class SkyMapPolygons(object):

    @staticmethod
    def makeBoxWcsRegion(box, wcs, margin=0.0):
        """Construct a spherical ConvexPolygon from a WCS and a bounding box.

        Parameters:
        -----------
        box : afw.geom.Box2I or afw.geom.Box2D
            A box in the pixel coordinate system defined by the WCS.
        wcs : afw.image.Wcs
            A mapping from a pixel coordinate system to the sky.
        margin : float
            A buffer in pixels to grow the box by (in all directions) before
            transforming it to sky coordinates.

        Returns a sphgeom.ConvexPolygon.
        """
        box = lsst.afw.geom.Box2D(box)
        box.grow(margin)
        vertices = []
        for point in box.getCorners():
            coord = wcs.pixelToSky(point)
            lonlat = lsst.sphgeom.LonLat.fromRadians(coord.getRa().asRadians(),
                                                     coord.getDec().asRadians())
            vertices.append(lsst.sphgeom.UnitVector3d(lonlat))
        return lsst.sphgeom.ConvexPolygon(vertices)

    def __init__(self, skyMap,db_path):
        self.skyMap = skyMap
        self.tracts = {}
        self.patches = {}
        tinfo_path = db_path.replace(os.path.basename(db_path),'tracts.pkl')
        if os.path.isfile(tinfo_path):
            with open(tinfo_path, 'rb') as handle:
                self.tracts = pickle.load(handle)
            print('retrieving tract info from %s'%tinfo_path)
        else:
            for n, tractInfo in enumerate(self.skyMap):
                if n % 100 == 0 and n > 0:
                    print("Prepping tract %d of %d" % (n, len(self.skyMap)))
                self.tracts[tractInfo.getId()] = self.makeBoxWcsRegion(
                    tractInfo.getBBox(),
                    tractInfo.getWcs()
                )
            with open(tinfo_path, 'wb') as handle:
                pickle.dump(self.tracts, handle)

    def _ensurePatches(self, tract):
        if tract not in self.patches:
            patches = {}
            tractInfo = self.skyMap[tract]
            for patchInfo in tractInfo:
                patches[patchInfo.getIndex()] = self.makeBoxWcsRegion(
                    patchInfo.getOuterBBox(),
                    tractInfo.getWcs()
                )
            self.patches[tract] = patches

    def findOverlaps(self, box, wcs, margin=100):
        polygon = self.makeBoxWcsRegion(box=box, wcs=wcs, margin=margin)
        results = []
        for tract, tractPoly in self.tracts.items():
            if polygon.relate(tractPoly) != lsst.sphgeom.DISJOINT:
                self._ensurePatches(tract)
                results.append(
                    (tract,
                     [patch for patch, patchPoly in self.patches[tract].items()
                      if polygon.relate(patchPoly) != lsst.sphgeom.DISJOINT])
                )
        return results


def main(db, butler, skyMapPolys, layer="", margin=10, verbose=True, visit=None):
    checkSql = "SELECT COUNT(*) FROM overlaps WHERE visit=? AND detector=?"
    insertSql = "INSERT INTO overlaps (tract, patch, visit, detector, filter, layer) VALUES (?, ?, ?, ?, ?, ?)"
    if visit is None:
        dataRefs = butler.subset("calexp")
    else:
        dataRefs = butler.subset("calexp", visit=int(visit))

    for dataRef in dataRefs:
        visit = dataRef.dataId["visit"]
        detector = dataRef.dataId["detector"]
        filter = dataRef.dataId["filter"]
        cursor = db.execute(checkSql, (visit, detector))
        if cursor.fetchone()[0]:
            if verbose:
                print("Skipping visit=%d, detector=%d: already present in overlap table." % (visit, detector))
            continue
        if not dataRef.datasetExists("calexp"):
            if verbose:
                print("Skipping visit=%d, detector=%d: no calexp found." % (visit, detector))
            continue
        wcs = dataRef.get("calexp_wcs")
        bbox = dataRef.get("calexp_bbox")
        for tract, patches in skyMapPolys.findOverlaps(bbox, wcs, margin=margin):
            print("Adding patches for visit=%d, detector=%d, tract=%d to overlap table." %
                  (visit, detector, tract))
            db.executemany(insertSql, [(tract, str(patch), visit, detector, filter, layer) for patch in patches])
    db.commit()

def _get_visits(visit_in):
    if os.path.isfile(visit_in):
        return np.loadtxt(visit_in, dtype=int)
    elif ',' in visit_in:
        return np.array(visit_in.split(','), dtype=int)
    else:
        return np.array([visit_in], dtype=int)

def _checkRequiredArguments(opts, parser):
    missing_options = []
    for option in parser.option_list:
        if option.help.startswith('^\[REQUIRED\]') and eval('opts.' + option.dest) == None:
            missing_options.extend(option._long_opts)
    if len(missing_options) > 0:
        parser.error('Missing REQUIRED parameters: ' + str(missing_options))

if __name__ == "__main__":
    usage = """%prog input [option]"""
    description = """Report tracts and patches containing calexps"""

    parser = OptionParser(description=description, usage=usage)
    parser.add_option("-v", "--visits", type="string",
                      help="[Required] List of visits (file or coma separated list)")
    parser.add_option("-i", "--indir", type="string",
                      help="[Required] Input directory for butler")
    parser.add_option("-d", "--db", type="string",
                      help="tractinfo database", default='overlaps.sqlite3')
    
    # parser.add_option("--ccdkey", type="string",
    #                   help="CCD key", default='sensor')
    # parser.add_option("-f", "--filt", type="string",
    #                   help="A filter name", default=None)
    opts, args = parser.parse_args()
    _checkRequiredArguments(opts, parser)

    butler = lsst.daf.persistence.Butler(opts.indir)
    skyMap = butler.get("deepCoadd_skyMap")
    skyMapPolys = SkyMapPolygons(skyMap,os.path.abspath(opts.db))

    visits = _get_visits(opts.visits)

    db = sqlite3.connect(opts.db)
    sql_create_projects_table = """ CREATE TABLE IF NOT EXISTS overlaps (
                                        id integer PRIMARY KEY,
                                        tract integer NOT NULL,
                                        patch text NOT NULL,
                                        visit integer NOT NULL,
                                        detector integer,
                                        filter text NOT NULL,
                                        layer text
                                    ); """
    db.cursor().execute(sql_create_projects_table)
    # visits = np.loadtxt('u_visit2.list', dtype=str) 
    for visit in visits:#[:,1]:
        #visit=visit.split('=')[1]
        #print(visit)
        main(db, butler, skyMapPolys, visit=visit)
    db.close()

