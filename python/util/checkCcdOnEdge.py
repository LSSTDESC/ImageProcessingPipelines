#!/usr/bin/env python3

#
# LSST Data Management System
# Copyright 2008-2016 AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <https://www.lsstcorp.org/LegalNotices/>.
#
import lsst.pex.config as pexConfig
import lsst.pipe.base as pipeBase
import lsst.daf.persistence as dafPersist
import lsst.afw.geom as afwGeom


__all__ = ["CheckCcdOnEdgeConfig", "CheckCcdOnEdgeTask"]

class CheckCcdOnEdgeConfig(pexConfig.Config):
    """Config for checkCcdOnEdge"""

    onEdgeCut = pexConfig.Field(
        dtype=float,
        default=1E-08,
        doc='CCD area not overlapping DC2 footprint (area > onEdgeCut) means CCD on edge'
    )

class CheckCcdOnEdgeTask(pipeBase.CmdLineTask):

    ConfigClass = CheckCcdOnEdgeConfig
    RunnerClass = pipeBase.ButlerInitializedTaskRunner
    _DefaultName = "checkCcdOnEdge"

    def __init__(self, butler=None, **kwargs):
        """!
        @param[in] butler  The butler is passed to the refObjLoader constructor in case it is
            needed.  Ignored if the refObjLoader argument provides a loader directly.
        @param[in,out] kwargs  other keyword arguments for lsst.pipe.base.CmdLineTask
        """
        pipeBase.CmdLineTask.__init__(self, **kwargs)
        self.butler = butler

    @classmethod
    def _makeArgumentParser(cls):
        """Create an argument parser"""

        parser = pipeBase.InputOnlyArgumentParser(name=cls._DefaultName)
        parser.add_id_argument("--id", "raw", help="Data ID, e.g. --id visit=6789")

        return parser

    # no saving of the config for now
    def _getConfigName(self):
         return None

    # not saving the metadata either
    def _getMetadataName(self):
        return None

    @pipeBase.timeMethod
    def bboxToRaDec(self, bbox, wcs):
        """Get the corners of a BBox and convert them to lists of RA and Dec."""
        corners = []
        for corner in bbox.getCorners():
            p = afwGeom.Point2D(corner.getX(), corner.getY())
            coord = wcs.pixelToSky(p)
            corners.append([coord.getRa().asDegrees(), coord.getDec().asDegrees()])
        ra, dec = zip(*corners)
        return ra, dec

    def runDataRef(self, sensorRef):
        """Process one CCD
        """
        dataid = sensorRef.dataId
        self.log.info("Processing %s" % (dataid))

        raw = self.butler.get('raw', dataid)
        wcsRaw = raw.getWcs()
        bbox = raw.getBBox()
        ra, dec = self.bboxToRaDec(bbox, wcsRaw)
        minPoint = afwGeom.Point2D(min(ra), min(dec))
        maxPoint = afwGeom.Point2D(max(ra), max(dec))
        bboxRaDec = afwGeom.Box2D(minPoint, maxPoint)

        # DC2 footprint
        NE = afwGeom.Point2D(71.46, -27.25)
        NW = afwGeom.Point2D(52.25, -27.25)
        SE = afwGeom.Point2D(73.79, -44.33)
        SW = afwGeom.Point2D(49.92, -44.33)
        polyList = [NE, NW, SW, SE]
        poly = afwGeom.Polygon(polyList)

        interArea = poly.intersection(bboxRaDec)[0].calculateArea()
        ccdArea = bboxRaDec.getArea()
        diff = abs(ccdArea-interArea)

        print(ccdArea, interArea, abs(ccdArea-interArea))

        if diff > self.config.onEdgeCut:
            self.log.info("CCD on the edge of DC2 footprint, diff area: %f"%diff)
        else:
            self.log.info("CCD is fully contained within the DC2 footprint")

        return diff

def main():
    CheckCcdOnEdgeTask.parseAndRun()

if __name__ == "__main__":
    main()
