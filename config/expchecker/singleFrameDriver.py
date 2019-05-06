
import os.path

from lsst.utils import getPackageDir

config.processCcd.load(os.path.join(getPackageDir("obs_lsst"), "config",
                                    "imsim", "processCcd.py"))

config.processCcd.charImage.repair.cosmicray.keepCRs=True
config.processCcd.isr.doSaturationInterpolation=False

