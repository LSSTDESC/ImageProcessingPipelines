
import os.path

from lsst.utils import getPackageDir

config.load(os.path.join(getPackageDir("obs_lsst"), "config",
                                    "imsim", "processCcd.py"))

config.charImage.repair.cosmicray.keepCRs=True
config.isr.doSaturationInterpolation=False

