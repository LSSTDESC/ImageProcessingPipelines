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
import lsst.afw.table as afwTable
import lsst.afw.geom as afwGeom
import lsst.afw.coord as afwCoord
import lsst.afw.image as afwImage
from lsst.meas.algorithms import LoadIndexedReferenceObjectsTask
from astropy.coordinates import SkyCoord
from astropy import units as u
import numpy as np


__all__ = ["CheckCcdAstrometryConfig", "CheckCcdAstrometryTask"]

class CheckCcdAstrometryConfig(pexConfig.Config):
    """Config for checkCcdAstrometry"""

    fluxType = pexConfig.Field(
        dtype=str,
        default='slot_ModelFlux',
        doc='type of flux to be used for magnitude estimate'
    )

    rejectCut = pexConfig.Field(
        dtype=float,
        default=120.0,
        doc='rejection cut on astrometric scatter (good < rejectCut)'
    )

    magCut = pexConfig.Field(
        dtype=float,
        default=22.0,
        doc='only consider object with mag < magCut'
    )

    refCat = pexConfig.Field(
        dtype=str,
        default='cal_ref_cat',
        doc='name of the reference catalog'
    )

class CheckCcdAstrometryTask(pipeBase.CmdLineTask):
    """!Assemble raw data, fit the PSF, detect and measure, and fit WCS and zero-point

    """
    ConfigClass = CheckCcdAstrometryConfig
    RunnerClass = pipeBase.ButlerInitializedTaskRunner
    _DefaultName = "checkCcdAstrometry"

    def __init__(self, butler=None, **kwargs):
        """!
        @param[in] butler  The butler is passed to the refObjLoader constructor in case it is
            needed.  Ignored if the refObjLoader argument provides a loader directly.
        @param[in,out] kwargs  other keyword arguments for lsst.pipe.base.CmdLineTask
        """
        pipeBase.CmdLineTask.__init__(self, **kwargs)
        self.butler = butler

        # Configure LoadIndexedReferenceObjectsTask
        refConfig = LoadIndexedReferenceObjectsTask.ConfigClass()
        refConfig.ref_dataset_name = self.config.refCat
        self.refTask = LoadIndexedReferenceObjectsTask(self.butler, config=refConfig)

    @classmethod
    def _makeArgumentParser(cls):
        """Create an argument parser"""

        parser = pipeBase.InputOnlyArgumentParser(name=cls._DefaultName)
        parser.add_id_argument("--id", "calexp", help="Data ID, e.g. --id visit=6789 (optional)")

        return parser

    # no saving of the config for now
    def _getConfigName(self):
         return None

    # not saving the metadata either
    def _getMetadataName(self):
        return None

    @pipeBase.timeMethod
    def runDataRef(self, sensorRef):
        """Process one CCD
        """
        dataid = sensorRef.dataId
        self.log.info("Processing %s" % (dataid))

        wcs = self.butler.get('calexp_wcs', dataid)
        calib = self.butler.get("calexp_calib", dataid)

        Flags = ["base_PixelFlags_flag_saturated", "base_PixelFlags_flag_cr", "base_PixelFlags_flag_interpolated",
                 self.config.fluxType + "_flag", "base_SdssCentroid_flag",
                 "base_SdssCentroid_flag_almostNoSecondDerivative", "base_SdssCentroid_flag_edge", "base_SdssCentroid_flag_noSecondDerivative",
                 "base_SdssCentroid_flag_notAtMaximum", "base_SdssCentroid_flag_resetToPeak",
                 "base_SdssShape_flag", "base_ClassificationExtendedness_flag"]

        src = self.butler.get('src', dataid).asAstropy()

        # get filter name associated to this visit
        for dataRef in self.butler.subset('src', visit=dataid['visit']):
            if dataRef.datasetExists():
                fullId = dataRef.dataId
            else:
                continue
        filt = fullId['filter']

        # select sources
        cut = np.ones_like(src['id'], dtype=bool)
        for flag in Flags:
            cut &= src[flag]==False
        cut &= (src[self.config.fluxType + '_instFlux'] > 0) & (src[self.config.fluxType + '_instFlux'] / src[self.config.fluxType + '_instFluxErr'] > 5)
        cut &= (src['base_ClassificationExtendedness_value'] == 0)

        mag, magErr = calib.getMagnitude(src[cut][self.config.fluxType + '_instFlux'], src[cut][self.config.fluxType + '_instFluxErr'])

        cat = src[cut]['id', 'coord_ra', 'coord_dec']
        cat['mag'] = mag
        cat['magErr'] = magErr

        cut = cat['mag'] < self.config.magCut
        cat = cat[cut]

        #define a reference filter (not critical for what we are doing)
        #f = 'lsst_' + filt + '_smeared'
        f = filt

        # Find the approximate celestial coordinates of the sensor's center
        centerPixel = afwGeom.Point2D(2000., 2000.)
        centerCoord = wcs.pixelToSky(centerPixel)

        # Retrieve reference object within a 0.5 deg radius circle around the sensor's center
        radius = afwGeom.Angle(0.5, afwGeom.degrees)
        ref = self.refTask.loadSkyCircle(centerCoord, radius, f).refCat.copy(deep=True).asAstropy()

        # create SkyCoord catalogs for astropy matching
        cRef = SkyCoord(ra = ref['coord_ra'], dec = ref['coord_dec'])
        cSrc = SkyCoord(ra = cat['coord_ra'], dec = cat['coord_dec'])

        # match catalogs
        idx, d2d, d3d = cSrc.match_to_catalog_sky(cRef)

        # get median distance between matched sources and references
        median = np.median(d2d.milliarcsecond)
        self.log.info('astrometric scatter:  %(visit)d  %(raftName)s  %(detectorName)s  ' % dataid
                      + '%.2f' % median)
        self.log.info("Median astrometric scatter %.2f mas" %(median))

        if median > self.config.rejectCut:
            self.log.error("Median astrometric scatter is too large %.2f mas astrometric fit probably failed" %(median))

        return median

def main():
    CheckCcdAstrometryTask.parseAndRun()

if __name__ == "__main__":
    main()
