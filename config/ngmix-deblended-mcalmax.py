# really should cut objects that have high mask fraction
# also maybe inexact_psf entirely, maybe not_deblended
# but that depends if we care about deblending.
# also might get caught by mask fraction?


# defaults to True, use the deblended coadds
#config.useDeblends = False


# optionally limit the number of objects we process
# the range is inclusive, not like a slice

# first index to process (defaults to 0).  Index could be an object
# index or a group index, etc.
#config.start_index = 1000
# number to process (defaults to everything after start_index)
#config.num_to_process = 10

# write out some diagnostic plots to the CWD, defaults to False
#config.make_plots: False
# a prefix to add to plot names
#config.plot_prefix='./plots/'

#
# postage stamp config
#

config.stamps.min_stamp_size = 32
config.stamps.max_stamp_size = 256
# how many "sigma" to make the radius of the box
config.stamps.sigma_factor = 5.0

# Jim comments "bright object" is just a geometric region around bright stars,
# intended mostly for people doing things like cross-correlations that could be
# impacted by e.g.  slight systematic effects in the background.  
#
# "clipped" means one or more input images was not used in this area (for large
# regions that's usually because of a ghost, I think) - but the PSF model acts
# as if that input image was used, so you'll probably see INEXACT_PSF set as
# well, which indicates that the pixels and the PSF model assume different
# things about the contributing images.
#
# I think I'd say for all of those that we should definitely proceed with
# photometric fitting, but whether we can do shear in those areas is
# questionable for bright star (I think the regions are probably conservatively
# large right now), and we definitely can't do shear in INEXACT_PSF areas.

# this is for our kludge to get the background noise
# by taking the median of the pixels without these bits
# set.  we should probably exclude the pixels that are part
# of the objects as well
config.stamps.bits_to_ignore_for_weight = [
    # for coadd assume OK since will be interpolated on one of the epochs
    'BAD',
    'SAT',
    'INTRP',
    'CR',
    'EDGE',
    'SUSPECT',
    'NO_DATA',
    #'BRIGHT_OBJECT',
    # only appears in coadd, may want to cut on this for shear
    #'CLIPPED',
    'CROSSTALK',
    # only matter if we are using the deblended photometry, which we are not here
    #'NOT_DEBLENDED',
    'UNMASKEDNAN',
]


# mask bits to null in the weight map. We need to think what we will do
# for metacal here
config.stamps.bits_to_null = [
    # for coadd assume OK since will be interpolated on one of the epochs
    #'BAD',
    'SAT',
    # Jim says "cut on the reason it was interpolated rather than this"
    #'INTRP',
    # for coadd assume OK since will be interpolated on one of the epochs
    #'CR',
    'EDGE',
    'SUSPECT',
    'NO_DATA',
    # Jim suggests we cut these after the fact in the bright star mask
    #'BRIGHT_OBJECT',
    # only appears in coadd, may want to cut on this for shear
    #'CLIPPED',
    #'CROSSTALK',
    # only matter if we are using the deblended photometry, which we are not here
    #'NOT_DEBLENDED',
    'UNMASKEDNAN',
]

# we do not process objects for which these bits are set
# not_deblended may not even matter for metacal, and may
# well cause a detection/selection bias we can't control
config.stamps.bits_to_cut = [
    'BRIGHT_OBJECT',
    'INEXACT_PSF',
    #'NOT_DEBLENDED',
]

# we will be nulling the weight map for some bits, don't allow more
# than this fraction to be masked in any band
config.stamps.max_zero_weight_frac = 0.05

###############
# object config

config.obj.model="gauss"

#
# prior on center
#

config.obj.priors.cen.type="gauss2d"

# this is the width in both directions
config.obj.priors.cen.pars=[0.2]

#
# prior on the ellipticity g
#

config.obj.priors.g.type="ba"

# this is the width
config.obj.priors.g.pars=[0.3]


#
# prior on the size squared T
#

config.obj.priors.T.type="two-sided-erf"

# this is the width
config.obj.priors.T.pars=[-10.0, 0.03, 1.0e+06, 1.0e+05]


#
# prior on the flux
#

config.obj.priors.flux.type="two-sided-erf"

# this is the width
config.obj.priors.flux.pars=[-1.0e+04, 1.0, 1.0e+09, 0.25e+08]

# fitting parameters
config.obj.max_pars.ntry=2
config.obj.max_pars.lm_pars.maxfev=2000
config.obj.max_pars.lm_pars.xtol=5.0e-5
config.obj.max_pars.lm_pars.ftol=5.0e-5


#############
# psf config

# a single gaussian
config.psf.model="gauss"
config.psf.fwhm_guess=0.8

# fitting parameters
config.psf.max_pars.ntry=4
config.psf.max_pars.lm_pars.maxfev=2000
config.psf.max_pars.lm_pars.xtol=5.0e-5
config.psf.max_pars.lm_pars.ftol=5.0e-5

# metacal pars (using the default)
#config.metacal.psf='fitgauss'
#config.metacal.types=['noshear','1p','1m','2p','2m']
