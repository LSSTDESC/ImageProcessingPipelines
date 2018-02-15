
.. raw:: html

   <!-- ![](./header.png) -->

The DC2 Cookbook: Recipes for Emulating the LSST DM Data Release Processing Pipeline
====================================================================================

*Simon Krughoff, Phil Marshall and others*

*This Note has not yet undergone DESC working group review, and hence
should be read with caution. To cite this work in progress, please use
"Krughoff et al 2018 in preparation" and provide a link to `this
development branch
URL <https://github.com/LSSTDESC/DC2_Repo/blob/issue/73/cookbook/Notebooks/DC2Cookbook.ipynb>`__.*

Introduction
------------

In this Note we describe the sequence of LSST data management (DM)
software stack calls needed to emulate the expected Data Release
Processing (DRP) pipeline. These recipes were initially developed for
the Twinkles project, the pathfinder for the first LSST DESC data
challenge, DC1, and were later adapted for the main DC1 simulation. We
provide links to the original DC1 recipes at the appropriate points
below.

The target DRP pipeline is described in the LSST Project document
`LDM-151 <ls.st/ldm-151>`__, *"LSST Data Management Science Pipelines
Design."* We will refer to this document extensively, importing figures
and quoting from it as needed. In DC1, the simulated images that we
produced were so-called "e-images", ecah one of which emulates a
calibrated frame; in DC2, we will produce amplifier (science) and
accompanying calibration images, and extend the Twinkles pipeline to
include instrument signature removal. We also adopt the recently
developed object association code, in order to make ``DIAObjects`` from
our ``DIASources``. Other than these two extensions, we expect the DC2
DM pipeline to look similar to the DC1 pipeline.

 This Note is organized as follows. We first provide an
`overview <#overview>`__ of the DRP pipeline, summarizing the relevant
section in `LDM-151 <ls.st/ldm-151>`__. Then, we present the recipes in
three sections: 1. `Image Coaddition and Object Detection <#coadds>`__
2. `Difference Image Analysis and DIASource Detection <#diasources>`__
3. `DIAObject Generation and Light Curve Forced
Photometry <#forcedphot>`__

We then provide some brief `concluding remarks <#conclusions>`__.

 ## Data Release Processing Overview

The DRP pipelines are summarized in `LDM-151 <ls.st/ldm-151>`__ by the
following Figure:

Anticipating that difference image analysis (DIA) will not be required
in the DC2 main survey area, it makes sense to preserve the DC1 grouping
of the DRP pipelines, into static sky and dynamic sky pipelines. \* The
static sky analysis involves image characterization, calibration and
coaddition followed by ``Object`` generation and measurement. \* The
dynamic sky analysis makes use of the calibrated images, but involves 1)
the construction of a ``TemplateCoadd`` image followed by image
differencing, ``DIASource`` detection in the difference images, and then
2) association of those ``DIASources`` into ``DIAObjects`` which then
form the target positions for forced photometry. Light curves can then
be extracted from the ``DIAForcedSource`` table.

At the time of writing, multi-epoch object characterization is not yet
available in the DM stack. As a result, the ``Objects`` are the same as
the "Preliminary Objects", and are made by detecting sources in the
``Coadd`` images.

Note that there will be difference image analysis (DIA) carried out in
both the Alert Production and Data Release Processing pipelines. The
main difference is that the APP will need to build ``DIAObjects`` on the
fly, while the DRP will have all data available to it at once, after the
fact. The DRP can make the best possible ``TemplateCoadd`` reference
image, and then generate new ``DIASources`` and aggregate them into new,
"reprocessed" ``DIAObjects`` ready for a single pass of forced
photometry at their locations of all these reprocessed ``DIAObjects``,
which will result in light curves extending over the duration of the
whole survey to that point. As in Twinkles, in DC2 our goal for the
dynamic sky analysis is to emulate the production of cosmology-ready
LSST supernova and lensed quasar light curves, which is why we retain
the focus on the *data release reprocessing* of the visit images.

`Back to the table of contents. <#toc>`__

 ## Image Coaddition and ``Object`` Detection

This recipe started life as the original Twinkles Cookbook recipe,
`*"Recipe: Emulating the DM Level 2
Pipeline"* <https://github.com/LSSTDESC/Twinkles/blob/master/doc/Cookbook/DM_Level2_Recipe.md>`__.
This was intended to show how to process simulated image data as if we
were running the static sky part of the LSST DM DRP. The primary
products are catalogs of ``Objects.``

Build the indexes for astrometric and photometric calibration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We use the ``phoSim`` reference catalogs to emulate the kind of high
accuracy calibration that we expect to be possible with the LSST data.
This is an approximation, but for many purposes a good one. **ACTION:
check that this is still the approach we want to take.**

**ACTION: Simon: replace the recipe below with the DC1 approach (which
did not use astrometry.net) following the scripts pointed out by Jim
`(issue #87) <https://github.com/LSSTDESC/DC2_Repo/issues/87>`__**

Currently the reference catalogs need to be formatted as astrometry.net
index files. I can convert the reference catalog produced by
``generatePhosimInput.py``, but there are a couple of precursor steps.
First, there is a bug in how phosim creates the nominal WCS (PHOSIM-18).
The result is that the WCS claims to be ICRS but ignores precession.
Since the matching algorithms assume we know approximately where the
telescope is pointing, they fail unless the catalogs are fixed.

It is easier to hack the reference catalog than to fix every WCS in
every image, so I just correct for the approximate precession and it
gets close enough that the matching algorithms fail (the WCS will still
be wrong, but we don't really care since we aren't comparing to external
catalogs).

.. code:: bash

    $> awk '{printf("%i, %f, %f, %f, %f, %f, %i, %i\n", $1, $2-0.0608766, $3-0.0220287, $4, $5,$6,$7,$8)}' twinkles_ref.txt >twinkles_ref_obs.txt

The first few lines look like this:

::

    #uniqueId, raJ2000, decJ2000, lsst_g, lsst_r, lsst_i, starnotgal, isvariable
    992887068676, 52.989609, -27.381822, 26.000570, 24.490695, 22.338254, 1, 0
    1605702564868, 53.002656, -27.356515, 27.732406, 26.371370, 25.372229, 1, 0
    1277139994628, 52.991627, -27.362006, 24.948391, 23.598418, 22.391097, 1, 0
    1704223204356, 53.017637, -27.326836, 23.914298, 22.938313, 22.539221, 1, 0
    1605697082372, 53.017005, -27.333503, 21.839375, 21.498586, 21.378259, 1, 0
    1605694183428, 52.988539, -27.326388, 25.324673, 24.003677, 23.221476, 1, 0
    1605694345220, 52.992405, -27.326471, 19.366450, 18.940676, 18.774756, 1, 0
    1277138139140, 52.994290, -27.333325, 24.185304, 22.843333, 21.513559, 1, 0
    1605701058564, 53.008024, -27.350062, 21.925079, 21.523769, 21.378805, 1, 0

Now we translate the text file into a FITS file for indexing. I decided
to change the column names from the default output by CatSim. Then you
can do the actual index generation. You'll need to set up a couple of
packages then run some scripts to do the formatting.

.. code:: bash

    $> setup astrometry_net
    $> setup pyfits
    $> text2fits.py -H 'id, ra, dec, g, r, i, starnotgal, isvariable' -s ', ' twinkles_ref_obs.txt twinkles_ref.fits -f 'kdddddjj'
    $> export P=0106160
    $> build-astrometry-index -i twinkles_ref.fits -o index-${P}00.fits -I ${P}00 -P 0 -S r -n 100 -L 20 -E -j 0.4 -r 1 > build-00.log
    $> build-astrometry-index -1 index-${P}00.fits -o index-${P}01.fits -I ${P}01 -P 1 -S r -L 20 -E -M -j 0.4 > build-01.log &
    $> build-astrometry-index -1 index-${P}00.fits -o index-${P}02.fits -I ${P}02 -P 2 -S r -L 20 -E -M -j 0.4 > build-02.log &
    $> build-astrometry-index -1 index-${P}00.fits -o index-${P}03.fits -I ${P}03 -P 3 -S r -L 20 -E -M -j 0.4 > build-03.log &
    $> build-astrometry-index -1 index-${P}00.fits -o index-${P}04.fits -I ${P}04 -P 4 -S r -L 20 -E -M -j 0.4 > build-04.log
    $> mkdir and_files
    $> mv index*.fits and_files
    $> cd and_files

The matcher needs to know which index files are available and what
columns to use for photometric calibration. These are specified using a
configuration file. This file goes in the ``and_files`` directory. It is
called ``andConfig.py`` and looks like this:

::

    root.starGalaxyColumn = "starnotgal"
    root.variableColumn = "isvariable"
    filters = ('u', 'g', 'r', 'i', 'z', 'y')
    root.magColumnMap = {'u':'g', 'g':'g', 'r':'r', 'i':'i', 'z':'i', 'y':'i'}
    root.indexFiles = ['index-010616000.fits',
    'index-010616001.fits',
    'index-010616002.fits',
    'index-010616003.fits',
    'index-010616004.fits']

**ACTION: edit this recipe so that the correct assumptions about the DM
stack installation are made. (`issue
#89 <https://github.com/LSSTDESC/DC2_Repo/issues/89>`__)**

.. raw:: html

   <!-- 

   ### Set up the data to run DM processing

   First you'll need to build the stack using tickets/DM-4302 of obs_lsstSim.  In order to patch a branch version onto a pre-existing stack you can do something like the following:

   1. Build a master stack.  I suggest using [lsstsw](https://confluence.lsstcorp.org/display/LDMDG/The+LSST+Software+Build+Tool).
   2. Set up the stack: e.g. `$> setup obs_lsstSim -t bNNNN`
   3. Clone the package you want to patch on top of your stack `$> clone git@github.com:lsst/obs_lsstSim.git; cd obs_lsstSim`
   4. Get the branch: `$> checkout tickets/DM-4302`
   5. Set up just (-j) the cloned package (since the rest of the packages are already set up): `$> setup -j -r .`
   6. Build the cloned package (this is necessary even for pure python packages): `$> scons opt=3`
   7. Optionally install it in your stack: `$> scons install declare`

   This assumes the simulated images have landed in a directory called ```images```
   in the current directory.  In the images directory, you'll need a ```_mapper``` file with contents
   ```python
   lsst.obs.lsstSim.LsstSimMapper
   ```

   The above file will tell the stack where to put the raw files and eimages.

   Setup the stack environment.  This will make the `LsstSimMapper` class available:
   ```bash
   $> setup obs_lsstSim
   ```

   Ingest the images from a directory called images to a repository called `input_data`.
   There are some config overrides in the `ingest.py` file.
   ```bash
   $> ingestImages.py images images/lsst_*.fits.gz --mode link --output input_data
   ```
   Now you are setup to process the data.

   -->

Process the image data using the DM stack
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Start here if you just want to exercise the DM stack. If you didn't
follow the steps above, first get the data and astrometry.net index
files from
`here <https://lsst-web.ncsa.illinois.edu/~krughoff/data/gri_data.tar.gz>`__.
Then untar the tarball in a working directory.

**ACTION: Update this part to reflect new calibration procedure, and
follow multi-band processing described in DM documentation
`here <http://doxygen.lsst.codes/stack/doxygen/x_masterDoxyDoc/pipe_tasks_multi_band.html>`__
`(issue #88) <https://github.com/LSSTDESC/DC2_Repo/issues/88>`__**

**ACTION: Include description of ISR, if we are starting with amplifier
images `(issue
#90) <https://github.com/LSSTDESC/DC2_Repo/issues/90>`__**

After you have the data, you can start following the steps below to get,
for example, forced photometry in three bands. First, set up the
reference catalog for photometric and astrometric calibration:

.. code:: bash

    $> setup -m none -r and_files astrometry_net_data

Create calibrated images from the input eimages. This will write to a
repository called output\_data. The --id argument defines the data to
operate on. In this case it means process all data (in this example the
g, r, and i bands) with visit numbers between 840 and 879. Missing data
will be skipped.

.. code:: bash

    $> processEimage.py input_data/ --id visit=840..879 --output output_data

Make a skyMap to use as the basis for the astrometic system for the
coadds. This can't be done up front because makeDiscreteSkyMap decides
how to build the patches and tracts for the skyMap based on the data.

.. code:: bash

    $> makeDiscreteSkyMap.py output_data/ --id visit=840..879 --output output_data

Coadds are done in two steps. Step one is to warp the data to a common
astrometric system. The following does that. The config option is to use
background subtracted exposures as inputs. You can also specify visits
using the ^ operator meaning 'and'.

.. code:: bash

    $> makeCoaddTempExp.py output_data/ --selectId visit=840..849 --id filter=r patch=0,0 tract=0 --config bgSubtracted=True --output output_data
    $> makeCoaddTempExp.py output_data/ --selectId visit=860..869 --id filter=g patch=0,0 tract=0 --config bgSubtracted=True --output output_data
    $> makeCoaddTempExp.py output_data/ --selectId visit=870..879 --id filter=i patch=0,0 tract=0 --config bgSubtracted=True --output output_data

This is the second step which actually coadds the warped images. The
doInterp config option is required if there are any NaNs in the image
(which there will be for this set since the images do not cover the
whole patch).

.. code:: bash

    $> assembleCoadd.py output_data/ --selectId visit=840..849 --id filter=r patch=0,0 tract=0 --config doInterp=True --output output_data
    $> assembleCoadd.py output_data/ --selectId visit=860..869 --id filter=g patch=0,0 tract=0 --config doInterp=True --output output_data
    $> assembleCoadd.py output_data/ --selectId visit=870..879 --id filter=i patch=0,0 tract=0 --config doInterp=True --output output_data

Detect sources in the coadd and then merge detections from multiple
bands.

.. code:: bash

    $> detectCoaddSources.py output_data/ --id tract=0 patch=0,0 filter=g^r^i --output output_data
    $> mergeCoaddDetections.py output_data/ --id tract=0 patch=0,0 filter=g^r^i --output output_data

Do measurement on the sources detected in the above steps and merge the
measurements from multiple bands.

.. code:: bash

    $> measureCoaddSources.py output_data/ --id tract=0 patch=0,0 filter=g^r^i --config measurement.doApplyApCorr=yes --output output_data
    $> mergeCoaddMeasurements.py output_data/ --id tract=0 patch=0,0 filter=g^r^i --output output_data

Use the detections from the coadd to do forced photometry on all the
single frame data.

.. code:: bash

    $> forcedPhotCcd.py output_data/ --id tract=0 visit=840..879 sensor=1,1 raft=2,2 --config measurement.doApplyApCorr=yes --output output_data

This final step is not really necessary: it results in a
``ForcedSource`` table whose utility is questionable. We expect the
light curves of supernovae to come from the forced photometry of the
``DIASources`` (see below). However, the forced photometry of the static
sky ``Objects`` may provide some useful comparisons, so we include it.

.. raw:: html

   <!-- Closing remarks from the Twinkles recipe:

   Once the forced photometry is done, you can look at the output by loading the measurements using the butler.  [This script](../../bin/plot_point_mags.py) shows how to start looking at the measurements.  It produces the following image.  I tried to fit both the systematic floor and the 5-sigma value for each of the bands.  Results are shown in the legend of the following image.

   ![Repeat figure](gri_err.png)

   You can also use the stack to make a color image from the three coadds.  See [colorim.py](../../bin/colorim.py) for the code to do this.  Note that you can also overplot the detections.

   [![Coadd thumbnail](rgb_coadd_thumb.png)](rgb_coadd.png)

   -->

`Back to the table of contents. <#toc>`__

 ## Difference Image Analysis and ``DIASource`` Detection

The dynamic sky analysis was treated in two steps in the Twinkles
pathfinder, partly because at that time ``DIAObjects`` were not yet
readily made. The first part of the difference image analysis stopped at
the generation of ``DIASources``, and it is this Twinkles recipe,
`*"Recipe: How to create DIASources using PSF Homogenized
coadds"* <https://github.com/LSSTDESC/Twinkles/blob/master/doc/Cookbook/Coadd_Diffim_Recipe.md>`__
that we adapt for DC2 here.

The basic sequence of operations is as follows:

-  Produce calibrated exposures
-  Produce the skyMap
-  Generate a PSF-matched ``TemplateCoadd`` to use as the DIA reference
   image
-  Produce the DIA sources using image differencing

Note that, as written, this would duplicate the ``processEimage.py``
step from the static sky recipe above. This is primarily because I found
that I couldn't use the ``calexp``\ s produced for the static sky
analysis. This probably means we'll want to switch to this new way of
producing calibrated exposures.

**ACTION: Simplify this recipe by re-using the calexps from the static
sky processing (`issue
#91 <https://github.com/LSSTDESC/DC2_Repo/issues/91>`__)**

Produce the ``calexp``\ s needed for DIA
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Set up your environment:

.. code:: bash

    $> export MYREPODIR=~/Twinkles/repos
    $> export MYWORKDIR=~/Twinkles
    $> export CALEXPDIR=$MYWORKDIR/fixed_psf_size
    $> export COADDDIR=$MYWORKDIR/matched_coadd
    $> export DIFFDIR=$MYWORKDIR/matched_diffim
    $> export RAWDATADIR=/global/cscratch1/sd/desc/twinkles/work/4/input
    $> export AND_DIR=/global/homes/d/desc/twinkles/trial/and_files_Phosim_Deep_Precursor
    $> source /global/common/cori/contrib/lsst/lsstDM/setupStack-12_1.sh
    $> cd $MYREPODIR
    $> cd obs_lsstSim
    $> git checkout twinkles_1
    $> setup -j -m none -r $AND_DIR astrometry_net_data
    $> cd $MYWORKDIR

Make the calibrated exposures:

::

    $> processEimage.py $RAWDATADIR --output $CALEXPDIR --id filter='r'

NB. This task must be configured to have fixed size PSF measuremnt
kernels *or the PSF matching in the next step doesn't work.* These
configs are provided by the ```processEimage.py``
config <https://github.com/lsst/obs_lsstSim/blob/twinkles_395/config/processEimage.py>`__
in the
`twinkles\_395 <https://github.com/lsst/obs_lsstSim/tree/twinkles_395>`__
branch of the ``obs_lsstSim`` repository. > This means that PSFEX cannot
be used as the PSF measurement algorithm in this task or the PSF
matching will not work.

Make the ``SkyMap`` [issue `#121 <https://github.com/LSSTDESC/DC2_Repo/issues/121>`__]
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The first thing we need to make is the sky map on which the coadded
images will be projected for all bands. In order to ensure that the sky
map will not depend on the list of input visits (their location on the
sky or on the time and place the reprocessing happens), the sky map
should be absolute (using ``makeSkyMap.py`` and the appropriate
configuration) instead of relative to an input field (as done with
``makeDiscreteSkyMap.py``). Making such a sky map will create thousands
of tracts and patches, that will be fixed on the sky and usable in
different reprocessing sessions.

One possibility could be to use the `Rings sky
map <https://github.com/lsst/skymap/blob/master/python/lsst/skymap/ringsSkyMap.py#L42>`__,
as done for the HSC data reprocessing with the LSST stack (and currently
for CFHT data reprocessing as well). In order to make this sky map, some
modifications of the ``lsstSimMapper.py``
`script <https://github.com/lsst/obs_lsstSim/blob/master/python/lsst/obs/lsstSim/lsstSimMapper.py>`__
must be done to match what has been done in ``hasMapper.py``
(`here <https://github.com/lsst/skymap/blob/master/python/lsst/skymap/ringsSkyMap.py#L42>`__
and
`here <https://github.com/lsst/obs_subaru/blob/master/python/lsst/obs/hsc/hscMapper.py#L286>`__).
A configuration file (``makeSkyMapConfig.py`` below) is then needed to
run ``makeSkyMap.py``, containing information on how to parametrize the
rings map. An example of such a configuration can be taken from the HSC
reprocessing
`repository <https://github.com/LSSTDESC/ReprocessingTaskForce/blob/master/config/w_2017_49/cfht/makeSkyMapConfig.py>`__,
which contains:

::

    config.skyMap.name = "rings"
    config.skyMap["rings"].numRings = 120
    config.skyMap["rings"].projection = "TAN"
    config.skyMap["rings"].tractOverlap = 1.0/60 # Overlap between tracts (degrees)
    config.skyMap["rings"].pixelScale = 0.185

After having adapted ``lsstSimMapper.py`` and getting the configuration
file ready, the command to run will be:

.. code:: bash

    $> makeSkyMap.py INPUTDIR --output OUTPUTDIR --configfile makeSkyMapConfig.py

The list of tracts/patches in which there is actually data (out of the
18937 tracts in that case) can be determined using a `hand-made
script <https://github.com/LSSTDESC/ReprocessingTaskForce/blob/master/scripts/reportPatchesWithImages.py>`__
developped in the context of CFHT data reprocessing (that might need
some improvement).

Make the ``TemplateCoadd``\ s
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Next, we make the ``CoaddTempExp``\ s. This requires a manual config
step. The seeing in the data varies from visit to visit. For image
differencing to work well in the current system, the template should
have sharper seeing than the science images. Thus, we choose a subset of
the calibrated visit images to construct the coadd. More data will give
us higher signal to noise, but a wider coadd PSF. Less data allows for a
sharper coadd PSF, but lower signal to noise. We have decided to
parameterize this choice by allowing the maximum acceptable seeing in
pixels, ``select.maxPsfFwhm``, to be set at runtime. The FWHM of the
model Psf, ``modelPsf.defaultFwhm``, also needs to be set, and must
reflect this choice. In concrete terms, ``modelPsf.defaultFwhm`` must be
equal to or greater than ``select.maxPsfFwhm``, and we recommend they be
set to be equal to minimize loss to the broader coadd PSF.

NB. The config file must specify the same `size for the
``modelPsf`` <https://github.com/lsst/obs_lsstSim/blob/twinkles_395/config/makeCoaddTempExp.py#L6>`__
as was specified for the Psf measurement kernel in the above step.

::

    $> makeCoaddTempExp.py $CALEXPDIR --config modelPsf.defaultFwhm=4.85 select.maxPsfFwhm=4.85\
    > --selectId filter='r' --id filter='r' --output $COADDDIR

    You might think that it would be easy to determine the value of the
    ``select.maxPsfFwhm`` parameter in code, but the match PSF and the
    selection threshold must be known at the same time, at least, in the
    current task setup. It would be possible to separate these two steps
    with a little more effort.

Now we can make the ``TemplateCoadd``:

.. code:: bash

    $> assembleCoadd.py $COADDDIR --selectId filter='r' --id filter='r' patch=0,0 tract=0 --output $COADDDIR

Difference the images
~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

    $> imageDifference.py $COADDDIR --templateId filter='r' --id filter='r' --output $DIFFDIR

At this point you will have a diffim and a catalog of ``DIAsources``.
Note that each of the images that went into the coadd will have
significant ringing in the diffim, because in these cases the template
will be deconvolved in ``ImageDifference.py`` to match the science PSF.
The
`config <https://github.com/lsst/obs_lsstSim/blob/twinkles_395/config/imageDifference.py>`__
for the ImageDifferenceTask turns on decorrelation of the noise in the
difference image.

`Back to the table of contents. <#toc>`__

 ## ``DIAObject`` Generation and Light Curve Forced Photometry

Now that we have a table of ``DIASources``, from differencing all visit
images against the same template image, we can make ``DIAObjects`` by
simple spatial association, and then measure their light curves via
forced photometry. In this way, the "reprocessing" of the visit images
that will take place during DRP is cleaner and simpler than the
iterative ``DIAObject`` definition that will take place during nightly
processing of the observations in the Alert Production Pipeline.

The following recipe was originally written for Twinkles as `*"Recipe:
Emulating the Level 1 Reprocessing of DIAObjects: Difference Image
Forced
Photometry"* <https://github.com/LSSTDESC/Twinkles/blob/master/doc/Cookbook/Reprocessed_DIAObjects_Recipe.md>`__.
It consists of the following steps:

1. Assemble ``DIAObjects`` from ``DIASources``
2. Calculate aggregate quantities for ``DIAObjects`` based on the
   constituent ``DIASources``
3. Feed the ``DIAObjects`` to a difference image forced photometry task
   to compute light curves for each ``DIAObject``, which can then be
   stored in a new ``DIASource`` table.

This sequence of steps represents a minimal subset of those defined in
the `LSST Data Products Definition Document,
LSE-163 <https://docushare.lsstcorp.org/docushare/dsweb/Get/LSE-163>`__.

Associating ``DIASources``
~~~~~~~~~~~~~~~~~~~~~~~~~~

In Twinkles, the two ways we looked at associating Level 1
``DIASources`` into reprocessed ``DIAObjects`` were to: \* Collect
``DIASources`` into ``DIAObjects`` by doing a close neighbor match, in
sequence, on each ``DIASource`` table, adding orphan ``DIASources`` back
to the reference ``DIAObject`` catalog and thus building up a set of
``DIAObjects`` with member ``DIASources``. Note that something like this
online algorithm will need to be carried out in Level 1 during
operations. \* Use a clustering algorithm to do post-facto association
based on the spatial distribution of all the ``DIASources``
simultaneously.

The second approach is likely closer to what will be done in the
production Level 2 system, but the LSST DM Stack already contains a
utility for executing the first technique. The ``afwTable.MultiMatch``
tool can take many ``SourceCatalogs`` and build up associations of the
``DIASources`` by repeated application of a proximity cut. We use this
pre-existing tool as our first go at emulating Level 2 association. This
will require a new ``Task`` to fetch the ``DIASource`` catalogs and feed
them through ``MultiMatch``.

**ACTION: Update the above with the new DM object association code
`(issue #92) <https://github.com/LSSTDESC/DC2_Repo/issues/92>`__**

Aggregate quantities for ``DIAObjects``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We will take the associated catalog from ``MultiMatch`` and compute
aggregate quantities for the columns that impact the forced photometry:
i.e. positions, flags, and the total number of ``DIASources`` associated
with the ``DIAObject``. The aggregate quantites will be persisted in a
new dataset ``reproDIAObjects``.

**ACTION: Update the above once new association code is included. Maybe
can be removed?**

Forced Photometry
~~~~~~~~~~~~~~~~~

A new task will read the ``reproDIAObjects`` catalog. For each
difference image, the task will force photometer at the location of each
``reproDIAObject``. For each difference image, the task will store the
forced photometry catalog in the ``reproDIASource`` dataset.

**ACTION: Include code to carry out and store forced photometry on
DIAobjects (`issue
#93 <https://github.com/LSSTDESC/DC2_Repo/issues/93>`__)**

Wish List
~~~~~~~~~

The above will be filled in as we implement the various pieces. We need:

-  A tool to add datasets to the ``obs_lsstSim`` dataset policy file;
-  A task to execute the forced photometry;
-  A ``reproDIASource`` dataset to persist the forced measurements in.

**ACTION: Make sure code cells include handling of the above items**

`Back to the table of contents. <#toc>`__

 ## Concluding Remarks

`Back to the table of contents. <#toc>`__
