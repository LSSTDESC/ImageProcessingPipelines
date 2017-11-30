#!/usr/bin/env python
import os
import copy
import argparse
import numpy as np
from lsst.sims.catalogs.definitions import InstanceCatalog
from lsst.sims.catUtils.baseCatalogModels import StarObj
from lsst.sims.catUtils.mixins import AstrometryStars, PhotometryStars
from lsst.sims.utils import ObservationMetaData

class DcRefCat(InstanceCatalog, AstrometryStars, PhotometryStars):
    column_outputs = ['uniqueId', 'raJ2000', 'decJ2000',
                      'lsst_u', 'lsst_g', 'lsst_r', 'lsst_i', 'lsst_z',
                      'lsst_y', 'isresolved', 'isvariable']
    default_columns = [('isresolved', 0, int), ('isvariable', 0, int)]
    transformations = {'raJ2000': np.degrees, 'decJ2000': np.degrees}
    default_formats = {'S': '%s', 'f': '%.8f', 'i': '%i'}

parser = argparse.ArgumentParser()
parser.add_argument('RA', type=float,
                    help='RA of region center (ICRS degrees)')
parser.add_argument('Dec', type=float,
                    help='Dec of region center (ICRS degrees)')
parser.add_argument('radius', type=float, help='Radius (degees) of region')
parser.add_argument('--outfile', type=str, default='ref_cat.txt',
                    help='Reference catalog filename.')
parser.add_argument('--outdir', type=str, default='.',
                    help='Output directory for reference catalog.')
args = parser.parse_args()

obs = ObservationMetaData(pointingRA=args.RA,
                          pointingDec=args.Dec,
                          boundType='circle',
                          boundLength=args.radius)

star_db = StarObj(database='LSSTCATSIM', host='fatboy.phys.washington.edu',
                  port=1433, driver='mssql+pymssql')

cat = DcRefCat(star_db, obs_metadata=obs)
file_name = os.path.join(args.outdir, args.outfile)
cat.write_catalog(file_name, chunk_size=10000)
