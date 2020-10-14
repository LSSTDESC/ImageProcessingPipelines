#!/usr/bin/env python

from __future__ import print_function, division, absolute_import

import sqlite3
#import argparse
import lsst.sphgeom
import lsst.geom
from lsst.daf.persistence import Butler
import numpy as np
import os
from optparse import OptionParser
import pickle

from astropy.stats import sigma_clipped_stats

class SkyMapPolygons(object):

    @staticmethod
    def makeBoxWcsRegion(box, wcs, margin=0.0):
        """Construct a spherical ConvexPolygon from a WCS and a bounding box.

        Parameters:
        -----------
        box : geom.Box2I or geom.Box2D
            A box in the pixel coordinate system defined by the WCS.
        wcs : afw.image.Wcs
            A mapping from a pixel coordinate system to the sky.
        margin : float
            A buffer in pixels to grow the box by (in all directions) before
            transforming it to sky coordinates.

        Returns a sphgeom.ConvexPolygon.
        """
        box = lsst.geom.Box2D(box)
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

def compute_conditions_data(dataRef):
    rafac = 2*np.sqrt(2*np.log(2))
    platescale = 0.199598  # arscec/px
    cal = dataRef.get('calexp')
    pcal = dataRef.get('calexp_photoCalib')
    cal_wcs = cal.getWcs()
    cal_box = cal.getBBox()
    cal_info = cal.getInfo().getVisitInfo()
    cal_psf = cal.getPsf()
    cal_var = cal.getVariance()

    mjd = cal_info.getDate().get()
    airmass = cal_info.getBoresightAirmass()

    psf_shape = cal_psf.computeShape()
    psf_img = cal_psf.computeImage()
    ixx, iyy, ixy = psf_shape.getParameterVector()
    # pow(ixx*iyy-ixy^2,0.25)
    psf_detradius = psf_shape.getDeterminantRadius()
    #sqrt(0.5*(ixx+iyy))
    psf_traradius = psf_shape.getTraceRadius()
    # to FHWM by multiplying by 2*math.sqrt(2*math.log(2))
    psf_detfhwm = psf_detradius*rafac
    psf_trafhwm = psf_traradius*rafac
    A_pxsq = 1./np.sum(psf_img.array**2)
    A_arsecsq = platescale**2 * A_pxsq

    c1 = cal_wcs.pixelToSky(x=cal_box.beginX, y=cal_box.beginY)
    c2 = cal_wcs.pixelToSky(x=cal_box.beginX, y=cal_box.endY)
    c3 = cal_wcs.pixelToSky(x=cal_box.endX, y=cal_box.beginY)
    c4 = cal_wcs.pixelToSky(x=cal_box.endX, y=cal_box.endY)
    ra1  = c1.getRa().asDegrees()
    dec1 = c1.getDec().asDegrees()
    ra2  = c2.getRa().asDegrees()
    dec2 = c2.getDec().asDegrees()
    ra3  = c3.getRa().asDegrees()
    dec3 = c3.getDec().asDegrees()
    ra4  = c4.getRa().asDegrees()
    ra4  = c4.getRa().asDegrees()
    dec4 = c4.getDec().asDegrees()

    varPlane = cal_var.array
    sigmaPlane = np.sqrt(varPlane)
    mean_var, median_var, std_var = sigma_clipped_stats(varPlane)
    mean_sig, median_sig, std_sig = sigma_clipped_stats(sigmaPlane)

    zeroflux = pcal.getInstFluxAtZeroMagnitude()
    trsf_zflux = 2.5*np.log10(zeroflux)
    zeroflux_njy = pcal.instFluxToNanojansky(zeroflux)
    trsf_zflux_njy = 2.5*np.log10(zeroflux_njy)
    calib_mean = pcal.getCalibrationMean()
    calib_err = pcal.getCalibrationErr()
    twenty_flux = pcal.magnitudeToInstFlux(20.)
    twentytwo_flux = pcal.magnitudeToInstFlux(22.)
    mag5sigma = trsf_zflux - 2.5 * np.log10(5 * median_sig * np.sqrt(A_pxsq))

    return [mjd,airmass,ixx,iyy,ixy,psf_detradius,psf_traradius,psf_detfhwm,\
        psf_trafhwm,A_pxsq,A_arsecsq,mag5sigma,ra1,dec1,ra2,dec2,ra3,dec3,ra4,dec4,\
        mean_var,median_var,std_var,mean_sig,median_sig,std_sig,zeroflux,trsf_zflux,\
        zeroflux_njy,trsf_zflux_njy,calib_mean,calib_err,twenty_flux,twentytwo_flux]

def main(db, butler, skyMapPolys, layer="", margin=10, verbose=True, visit=None):
    checkSql = "SELECT COUNT(*) FROM overlaps WHERE visit=? AND detector=?"
    insertSql = "INSERT INTO overlaps (tract, patch, visit, detector, filter, layer) VALUES (?, ?, ?, ?, ?, ?)"
    conditions_vars="mjd,airmass,psf_ixx,psf_iyy,psf_ixy,psf_detradius,psf_traradius,psf_detfhwm,\
    psf_trafhwm,a_pxsq,a_arsecsq,mag5sigma,ccd_corner_1_ra,ccd_corner_1_dec,ccd_corner_2_ra,\
    ccd_corner_2_dec,ccd_corner_3_ra,ccd_corner_3_dec,ccd_corner_4_ra,ccd_corner_4_dec,\
    mean_variance,median_variance,std_variance,mean_sig,median_sig,std_sig,zeroflux,trsf_zflux,\
    zeroflux_njy,trsf_zflux_njy,calib_mean,calib_err,twenty_flux,twentytwo_flux"
    s="".join(['?, ' for i in range(37)])
    insertSql2 = "INSERT INTO conditions (visit, detector, filter, {}) VALUES ({})".format(conditions_vars,s[:-2])
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
            #live with intermittent locking of the db....
            while True:
                try:
                    db.executemany(insertSql, [(tract, str(patch), visit, detector, filter, layer) for patch in patches])
                    break
                except sqlite3.Error as e:
                    pass
                else:
                    break
        
        data_list = compute_conditions_data(dataRef)
        db.execute(insertSql2, [visit,detector,filter]+data_list)
    
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
                      help="[Required] Optional list of visits (file or coma separated list)")
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
    sql_create_projects_table = """ CREATE TABLE IF NOT EXISTS conditions (
                                        id integer PRIMARY KEY,
                                        visit integer NOT NULL,
                                        detector integer,
                                        filter text NOT NULL,
                                        mjd integer NOT NULL,
                                        airmass float NOT NULL,
                                        psf_ixx float NOT NULL,
                                        psf_iyy float NOT NULL, 
                                        psf_ixy float NOT NULL, 
                                        psf_detradius float NOT NULL, 
                                        psf_traradius float NOT NULL, 
                                        psf_detfhwm float NOT NULL, 
                                        psf_trafhwm float NOT NULL, 
                                        a_pxsq float NOT NULL,
                                        a_arsecsq float NOT NULL, 
                                        mag5sigma float NOT NULL, 
                                        ccd_corner_1_ra float NOT NULL, 
                                        ccd_corner_1_dec float NOT NULL, 
                                        ccd_corner_2_ra float NOT NULL, 
                                        ccd_corner_2_dec float NOT NULL, 
                                        ccd_corner_3_ra float NOT NULL, 
                                        ccd_corner_3_dec float NOT NULL, 
                                        ccd_corner_4_ra float NOT NULL, 
                                        ccd_corner_4_dec float NOT NULL, 
                                        mean_variance float NOT NULL, 
                                        median_variance float NOT NULL, 
                                        std_variance float NOT NULL, 
                                        mean_sig float NOT NULL, 
                                        median_sig float NOT NULL, 
                                        std_sig float NOT NULL, 
                                        zeroflux float NOT NULL, 
                                        trsf_zflux float NOT NULL, 
                                        zeroflux_njy float NOT NULL, 
                                        trsf_zflux_njy float NOT NULL, 
                                        calib_mean float NOT NULL, 
                                        calib_err float NOT NULL, 
                                        twenty_flux float NOT NULL, 
                                        twentytwo_flux float NOT NULL 
                                       ); """
    db.cursor().execute(sql_create_projects_table)


    # visits = np.loadtxt('u_visit2.list', dtype=str) 
    for visit in visits:#[:,1]:
        #visit=visit.split('=')[1]
        #print(visit)
        main(db, butler, skyMapPolys, visit=visit)
    db.close()

