# Configuration file for measureCoaddSources

import lsst.meas.extensions.shapeHSM
hsm_plugins = set([
    "ext_shapeHSM_HsmShapeBj",      # Bernstein & Jarvis 2002
    "ext_shapeHSM_HsmShapeLinear",  # Hirata & Seljak 2003
    "ext_shapeHSM_HsmShapeKsb",     # KSB 1995
    "ext_shapeHSM_HsmShapeRegauss", # Hirata & Seljak 2003
    "ext_shapeHSM_HsmSourceMoments",# Not PSF corrected; used by all of the above
    "ext_shapeHSM_HsmPsfMoments",   # Moments of the PSF, used by all of the above
])
config.measurement.plugins.names |= hsm_plugins

config.measurement.plugins['ext_shapeHSM_HsmShapeRegauss'].deblendNChild=''
config.measurement.slots.shape = "ext_shapeHSM_HsmSourceMoments"

import lsst.meas.modelfit
import lsst.shapelet
import lsst.meas.extensions.convolved # noqa: Load flux.convolved algorithm
#import lsst.meas.extensions.photometryKron
#    root.algorithms.names |= ["flux.kron"]
config.measurement.plugins.names |= ["modelfit_DoubleShapeletPsfApprox",
                                     "modelfit_CModel",
                                     "ext_convolved_ConvolvedFlux"]
config.measurement.plugins["ext_convolved_ConvolvedFlux"].seeing.append(8.0)
config.measurement.slots.modelFlux = "modelfit_CModel"

config.doApCorr=True

# Name of the ingested reference dataset                                                                                                                                                                                                      
config.match.refObjLoader.ref_dataset_name='sdss'

# Mapping of camera filter name: reference catalog filter name; each reference filter must exist                                                                                                                                              
config.match.refObjLoader.filterMap={
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

# Maximum linear dimension for footprints before they are ignored as large; non-positive means no threshold applied
config.deblend.maxFootprintSize=2000  # same as for processCcd
