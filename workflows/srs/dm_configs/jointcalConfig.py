# Configuration file for jointcal

from lsst.meas.algorithms import LoadIndexedReferenceObjectsTask

import os.path
from lsst.utils import getPackageDir

# Astrometry (copied from lsst:obs_subaru/config/processCcd.py)
for refObjLoader in (config.astrometryRefObjLoader,
                     config.photometryRefObjLoader):
    refObjLoader.retarget(LoadIndexedReferenceObjectsTask)
    refObjLoader.load(os.path.join(getPackageDir('obs_lsstSim'), 'config', 'filterMap.py'))

config.doPhotometry = False   # comment out to run the photometric calibration

# These are the default values

# Minimum allowed signal-to-noise ratio for sources used for matching
# (in the flux specified by sourceFluxType); <= 0 for no limit
# config.sourceSelector['matcher'].minSnr = 40.0

# Minimum allowed signal-to-noise ratio for sources used for matching
# (in the flux specified by sourceFluxType); <= 0 for no limit
config.sourceSelector['astrometry'].minSnr = 40.0  # default is 10


