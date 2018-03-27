# Apply the brighter fatter correction
config.processCcd.isr.doBrighterFatter=False

config.processCcd.charImage.repair.cosmicray.nCrPixelMax=1000000

# Useul to get to avoid deblending of satellite tracks
config.processCcd.calibrate.deblend.maxFootprintSize=2000 # 2200

# Use psfex instead of pca
import lsst.meas.extensions.psfex.psfexPsfDeterminer
config.processCcd.charImage.measurePsf.psfDeterminer.name='psfex'

# The following should be included for u filter in order to lower the source detection threshold
config.processCcd.charImage.detection.includeThresholdMultiplier=1.0

# Run CModel
import lsst.meas.modelfit
import lsst.meas.extensions.convolved  # noqa: Load flux.convolved algorithm
config.processCcd.charImage.measurement.plugins.names |= ["modelfit_DoubleShapeletPsfApprox", "modelfit_CModel", "ext_convolved_ConvolvedFlux"]

# Run astrometry using the new htm reference catalog format
# The following retargets are necessary until the new scheme becomes standard
from lsst.meas.algorithms import LoadIndexedReferenceObjectsTask
config.processCcd.calibrate.astromRefObjLoader.retarget(LoadIndexedReferenceObjectsTask)
config.processCcd.calibrate.photoRefObjLoader.retarget(LoadIndexedReferenceObjectsTask)

# Use new astrometry fitter
from lsst.meas.astrom import FitSipDistortionTask
config.processCcd.calibrate.astrometry.wcsFitter.retarget(FitSipDistortionTask)

config.processCcd.calibrate.astrometry.wcsFitter.order = 3
config.processCcd.calibrate.astrometry.matcher.maxMatchDistArcSec=5

# Select external catalogs for Astrometry and Photometry
config.processCcd.calibrate.photoRefObjLoader.ref_dataset_name='sdss'
#config.processCcd.calibrate.astromRefObjLoader.ref_dataset_name='gaia'
config.processCcd.calibrate.astromRefObjLoader.ref_dataset_name='pan-starrs'
#config.processCcd.calibrate.astromRefObjLoader.ref_dataset_name='sdss'

# Astrometry with panstarrs
config.processCcd.calibrate.astromRefObjLoader.filterMap = {
    'u':'g',
    'g':'g',
    'r':'r',
    'r2':'r',
    'i':'i',
    'i2':'i',
    'i3':'i',
    'z':'z',
    'y':'y',
}
# Astrometry with gaia
#config.processCcd.calibrate.astromRefObjLoader.filterMap = {
#    'u':'phot_g_mean_mag',
#    'g':'phot_g_mean_mag',
#    'r':'phot_g_mean_mag',
#    'i':'phot_g_mean_mag',
#    'z':'phot_g_mean_mag',
#    'y':'phot_g_mean_mag',
#}
# Photometry with sdss
config.processCcd.calibrate.photoRefObjLoader.filterMap = {
    'u': 'U',
    'g': 'G',
    'r': 'R',
    'r2': 'R',
    'i': 'I',
    'i2': 'I',
    'i3': 'I',
    'z': 'Z',
    'y': 'Z',
}

#Astrometry with sdss
#config.processCcd.calibrate.astromRefObjLoader.filterMap = {
#    'u': 'U',
#    'g': 'G',
#    'r': 'R',
#    'i': 'I',
#    'z': 'Z',
#    'y': 'Z',
#}

import lsst.pipe.tasks.colorterms
config.processCcd.calibrate.photoCal.colorterms.data['e2v'].data['i2']=lsst.pipe.tasks.colorterms.Colorterm()
config.processCcd.calibrate.photoCal.colorterms.data['e2v'].data['i2'].c2=0.0
config.processCcd.calibrate.photoCal.colorterms.data['e2v'].data['i2'].c1=0.003
config.processCcd.calibrate.photoCal.colorterms.data['e2v'].data['i2'].c0=0.0
config.processCcd.calibrate.photoCal.colorterms.data['e2v'].data['i2'].primary='i'
config.processCcd.calibrate.photoCal.colorterms.data['e2v'].data['i2'].secondary='r'
config.processCcd.calibrate.photoCal.colorterms.data['e2v'].data['i3']=lsst.pipe.tasks.colorterms.Colorterm()
config.processCcd.calibrate.photoCal.colorterms.data['e2v'].data['i3'].c2=0.0
config.processCcd.calibrate.photoCal.colorterms.data['e2v'].data['i3'].c1=0.003
config.processCcd.calibrate.photoCal.colorterms.data['e2v'].data['i3'].c0=0.0
config.processCcd.calibrate.photoCal.colorterms.data['e2v'].data['i3'].primary='i'
config.processCcd.calibrate.photoCal.colorterms.data['e2v'].data['i3'].secondary='r'
config.processCcd.calibrate.photoCal.colorterms.data['e2v'].data['r2']=lsst.pipe.tasks.colorterms.Colorterm()
config.processCcd.calibrate.photoCal.colorterms.data['e2v'].data['r2'].c2=0.0
config.processCcd.calibrate.photoCal.colorterms.data['e2v'].data['r2'].c1=0.024
config.processCcd.calibrate.photoCal.colorterms.data['e2v'].data['r2'].c0=0.0
config.processCcd.calibrate.photoCal.colorterms.data['e2v'].data['r2'].primary='r'
config.processCcd.calibrate.photoCal.colorterms.data['e2v'].data['r2'].secondary='g'

# use Chebyshev background estimation
config.processCcd.charImage.background.useApprox=True
config.processCcd.charImage.detection.background.binSize=128
config.processCcd.charImage.detection.background.useApprox=True
config.processCcd.charImage.background.binSize = 128
config.processCcd.charImage.background.undersampleStyle = 'REDUCE_INTERP_ORDER'
config.processCcd.charImage.detection.background.binSize = 128
config.processCcd.charImage.detection.background.undersampleStyle='REDUCE_INTERP_ORDER'
config.processCcd.charImage.detection.background.binSize = 128
config.processCcd.charImage.detection.background.undersampleStyle = 'REDUCE_INTERP_ORDER'

# Convolved fluxes can fail for small target seeing if the observation seeing is larger
if "ext_convolved_ConvolvedFlux" in config.processCcd.charImage.measurement.plugins:
    config.processCcd.charImage.measurement.plugins["ext_convolved_ConvolvedFlux"].seeing.append(8.0)
    names = config.processCcd.charImage.measurement.plugins["ext_convolved_ConvolvedFlux"].getAllResultNames()
    config.processCcd.charImage.measureApCorr.allowFailure += names