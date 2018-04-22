import lsst.meas.modelfit
import lsst.shapelet

config.measurement.plugins.names |= ["modelfit_GeneralShapeletPsfApprox", "modelfit_DoubleShapeletPsfApprox", "modelfit_CModel"]
config.measurement.slots.modelFlux = "modelfit_CModel"

config.doApCorr=True

#config.measurement.plugins['base_PixelFlags'].masksFpAnywhere.append('CLIPPED')
# These do not work anymore??
#config.measurement.plugins['base_PixelFlags'].masksFpCenter.append('BRIGHT_OBJECT')
#config.measurement.plugins['base_PixelFlags'].masksFpAnywhere.append('BRIGHT_OBJECT')
