#!/usr/bin/env python
# workflow.py - Main Parsl script for DESC DRP workflow

import concurrent.futures
import functools
import logging
import os
import re

import parsl

import checkpointutil  # noqa: F401 - for import-time checkpoint config
import configuration
import ingest
import tracts
import visits

from lsst_apps import lsst_app2
from workflowutils import read_and_strip
from future_combinators import combine


#############################
## Setup and Initialization
#############################
# PROCESSING FLAGS
doIngest = False     # switch to enable the ingest step, if True
doSkyMap = False     # switch to enable sky map creation, if True
doSensor = False     # switch to enable sensor/raft level processing, if True
doSqlite = True      # switch to enable the surprisingly time-consuming sqlite queries against the tracts_mapping db


# Establish logging
logger = logging.getLogger("parsl.workflow")
parsl.set_stream_logger(level=logging.INFO)
logger.info("WFLOW: Parsl driver for DM (DRP) pipeline")

# Read in workflow configuration
configuration = configuration.load_configuration()

# TODO:
# restarts by reruns
# this should go into the user config file but I'll prototype it here

# some of groups of steps of the broad workflow should have their own
# rerun directory in the repo.

# however because some are in parallel, they can't be entirely separate
# because they need to be linear (unless there is interesting rerun-merging
# magic)
# This approach then limits concurrency, perhaps?


# The rerun name for each step should include the previous steps, automatically
# so the step 6 rerun will be long.
rerun1_name = "1"  # contains outputs of: ingest and skymap
rerun2_name = "2"  # contains outputs of: singleFrameDriver
rerun3_name = "3"  # ... etc
rerun4_name = "4"
rerun5_name = "5"

visit_min = configuration.visit_min
visit_max = configuration.visit_max

tract_subset = configuration.tract_subset
patch_subset = configuration.patch_subset

rerun1 = configuration.rerun_prefix+rerun1_name
rerun2 = rerun1 + "." + rerun2_name
rerun3 = rerun2 + "." + rerun3_name
rerun4 = rerun3 + "." + rerun4_name
rerun5 = rerun4 + "." + rerun5_name


# Metadata is stored in the repo rerun subdirectory, but there
# is nothing "rerun"-like about it.

metadata_dir = os.path.join(configuration.repo_dir, 'rerun', configuration.rerun_prefix+'metadata')
if not os.path.exists(metadata_dir):
    os.makedirs(metadata_dir)
    

logger.info("WFLOW: Output to rerun/"+rerun1+" (etc)")

# Initialize Parsl
parsl.load(configuration.parsl_config)

# tell wrapper about parsl run_dir which isn't decided until
# after parsl.load()
configuration.wrap = functools.partial(configuration.wrap,
                                       run_dir=parsl.dfk().run_dir)
# Define Parsl log directory
logdir = parsl.dfk().run_dir + "/dm-logs/"
logger.info("WFLOW: Log directory is " + logdir)


# This is a list of futures which should be waited on at the end
# of the workflow before exiting (rather than being intermediate
# results used by subsequent steps)
terminal_futures = []



####################################
## Prepare for Ingest and Sky Map
####################################
# INGEST
# (old) ingest_future = ingest.perform_ingest(configuration)
# (new) ingest_future = ingest.perform_ingest(configuration, logdir)
# logger.info("waiting for ingest(s) to complete")
# ingest_future.result()
# logger.info("ingest(s) completed")

if doIngest:
    ingest_future = ingest.perform_ingest(configuration, logdir, rerun1)
else:
    logger.warning("WFLOW: Skip ingest")


# now equivalent of DC2DM_2_SINGLEFRAME_NERSC.xml

# setup_calexp .... eg workflows/srs/pipe_setups/setup_calexp

#   makeSkyMap.py
#   QUESTION: in xml, this does copying of files out of one rerun dir into
#   another, neither of which is the rerun dir passed to makeSkyMap...
#   what is going on there? I'm going to ignore reruns entirely here if
#   i can...

# QUESTION: what is the concurrency between make_sky_map and the raw visit
# list? can they run concurrently or must make_sky_map run before generating
# the raw visit list?

# ingest list is passed in but not used explicity because it represents
# that some stuff # has gone into the DB potentially during ingest
# - for checkpointing

# QUESTION: makeDiscreteSkyMap mentioned in
# https://pipelines.lsst.io/getting-started/coaddition.html
# sounds like it needs images to have been imported first so that the sky
# map covers the right amount of the sky. Is that the case here? is so,
# there needs to be a new dependency added.
@lsst_app2
def make_sky_map(repo_dir, rerun, stdout=None, stderr=None, wrap=None, parsl_resource_specification=None):
    return wrap("/usr/bin/time -v makeSkyMap.py {} --rerun {}".format(repo_dir, rerun))


# TODO: this can run in parallel with ingest
if doSkyMap:
    logger.info("WFLOW: launching makeSkyMap")
    skymap_future = make_sky_map(configuration.repo_dir, rerun1,
                                 stdout=logdir+"make_sky_map.stdout",
                                 stderr=logdir+"make_sky_map.stderr",
                                 wrap=configuration.wrap,
                                 parsl_resource_specification={"priority": (1000,)})
else:
    logger.warning("WFLOW: skipping makeSkyMap step")



###################################
## Ingest
###################################
if doIngest:
    logger.info("WFLOW: waiting for ingest(s) to complete")
    ingest_future.result()
    logger.info("WFLOW: ingest(s) completed")
else:
    logger.warning("WFLOW: skip data ingest.")
    pass



###################################
## Sky Map
###################################
if doSkyMap:
    logger.info("WFLOW: waiting for makeSkyMap to complete")
    skymap_future.result()
    logger.info("WFLOW: makeSkyMap completed")
    pass



###################################
## Prepare Sensor/Raft processing
###################################
@lsst_app2
def make_visit_file(repo_dir, visit_file, stdout=None, stderr=None, wrap=None, parsl_resource_specification=None):
    return wrap(('/usr/bin/time -v sqlite3 {repo_dir}/registry.sqlite3 '
                 '"SELECT DISTINCT visit FROM raw_visit;" '
                 '> {visit_file}').format(repo_dir=repo_dir,
                                          visit_file=visit_file))


@lsst_app2
def raft_list_for_visit(repo_dir, visit_id, out_filename,
                        stderr=None, stdout=None, wrap=None, parsl_resource_specification=None):
    return wrap(("/usr/bin/time -v sqlite3 {repo_dir}/registry.sqlite3 "
                 "'SELECT DISTINCT raftName FROM raw WHERE visit={visit_id}' "
                 "> {out_filename}").format(repo_dir=repo_dir,
                                            visit_id=visit_id,
                                            out_filename=out_filename))


# the parsl checkpointing for this won't detect if we ingested more stuff
# to do with the specified visit - see comments for check_ccd_astrometry
@lsst_app2
def tract2visit_mapper(dm_root, repo_dir, rerun, visit, inputs=[],
                       stderr=None, stdout=None, wrap=None,
                       parsl_resource_specification=None):
    # TODO: this seems to be how $REGISTRIES is figured out (via $WORKDIR)
    # perhaps? I'm unsure though
    registries = "{repo_dir}/rerun/{rerun}".format(repo_dir=repo_dir,
                                                   rerun=rerun)

    # the srs workflow has a separate output database per visit, which is
    # elsewhere merged into a single DB. That's awkward... there's probably
    # a reason to do with concurrency or shared fs that needs digging into.
    return wrap("mkdir -p {registries} && /usr/bin/time -v {dm_root}/ImageProcessingPipelines/python/util/tract2visit_mapper.py --indir={repo_dir}/rerun/{rerun} --db={registries}/tracts_mapping.sqlite3 --visits={visit}".format(repo_dir=repo_dir, rerun=rerun, visit=visit, registries=registries, dm_root=dm_root))


@parsl.python_app(executors=['submit-node'], join=True)
def process_visit_rafts(visit_id, raft_list_fn, inputs=None):

    raft_lines = read_and_strip(raft_list_fn)

    logger.info("WFLOW: => There are " + str(len(raft_lines)) + " rafts to process:")
    logger.info("WFLOW: "+str(raft_lines))

    this_visit_single_frame_futs = []
    this_visit_sky_correction_futs = []

    for (m, raft_name) in zip(range(0, len(raft_lines)), raft_lines):
        logger.info("WFLOW: visit {} raft {}".format(visit_id, raft_name))

        # this call is based on run_calexp shell script
        # assume visit_id really is a visit id... workflows/srs/pipe_setups/setup_calexp has a case where the visit file has two fields per line, and this is handled differently there. I have ignored that here.
        # raft_name is the $RAFTNAME environment variable in run_calexp in the XML workflows
        sfd_output_basename = "single_frame_driver.visit-{}.raft-{}".format(visit_id, raft_name)
        this_raft_single_frame_fut = visits.single_frame_driver(
            configuration.repo_dir,
            rerun1 + ":" + rerun2,
            visit_id,
            raft_name,
            stdout=logdir+sfd_output_basename+".stdout",
            stderr=logdir+sfd_output_basename+".stderr",
            wrap=configuration.wrap,
            parsl_resource_specification={"priority": (1200, visit_id)})
        # this is invoked in run_calexp with $OUT_DIR at the first parameter, but that's not something
        # i've used so far -- so I'm using IN_DIR as used in previous steps
        # TODO: is that the right thing to do? otherwise how does IN_DIR and OUT_DIR differ?
        sky_correction_stdbase = "sky_correction.visit-{}.raft-{}".format(visit_id, raft_name)
        this_visit_sky_correction_futs.append(visits.sky_correction(
            configuration.repo_dir,
            rerun2 + ":" + rerun3,
            visit_id,
            raft_name,
            inputs=[this_raft_single_frame_fut],
            stdout=logdir+sky_correction_stdbase+".stdout",
            stderr=logdir+sky_correction_stdbase+".stderr",
            wrap=configuration.wrap,
            parsl_resource_specification={"priority": (1100, visit_id)}))

    # now need to join based on all of this_visit_single_frame_futs... but not in sequential code
    # because otherwise we won't launch later visits until after we're done with this one, and
    # lose parallelism
    # question here: should these be done per-raft or per-visit?
    # the workflow looks like you can rnu with a single vist-raft but
    # then the subsequent
    # steps don't take raft as a parameter? so what's the deal there?
    # TODO: assume for now we need to wait for all rafts to be done,
    # and process per visit

    # TODO: which of these post-processing steps need to happen in
    # sequence rather than in parallel?

    check_ccd_stdbase = "check_ccd_astrometry.{}".format(visit_id)
    fut_check_ccd = visits.check_ccd_astrometry(
        configuration.dm_root,
        configuration.repo_dir,
        rerun3,
        visit_id,
        inputs=this_visit_sky_correction_futs,
        stdout=logdir+check_ccd_stdbase+".stdout",
        stderr=logdir+check_ccd_stdbase+".stderr",
        wrap=configuration.wrap)

    # some caution on re-running this: the DB is additive, I think, so if there
    # is stuff for this visit already in the DB from a previous run, it will be
    # added to here, leaving potentially wrong stuff in there if we've changed
    # things in the wrong way. That's a general note on adding in more stuff to
    # a run, though?
    tract2visit_mapper_stdbase = "tract2visit_mapper.{}".format(visit_id)
    fut_tract2visit = tract2visit_mapper(
        configuration.dm_root,
        configuration.repo_dir,
        rerun3,
        visit_id,
        inputs=this_visit_single_frame_futs,
        stdout=logdir+tract2visit_mapper_stdbase+".stdout",
        stderr=logdir+tract2visit_mapper_stdbase+".stderr",
        wrap=configuration.wrap,
        parsl_resource_specification={"priority": (1200, visit_id)})

    # This could go into terminal futures or we could explicitly wait for it
    # here. By waiting for it here, we ensure that the check has passed
    # before doing anything with the information.
    # By not waiting for it, we increase parallelism, but we might encounter
    # a downstream problem of some kind before discovering check_ccd is
    # broken?
    terminal_futures.append(fut_check_ccd)

    return combine(inputs=[fut_tract2visit] + this_visit_sky_correction_futs)


@parsl.python_app(executors=['submit-node'], join=True)
def process_visit(visit_id):
    logger.info("WFLOW: => Begin processing visit "+str(visit_id))

    # some of this stuff could probably be parallelised down to the per-sensor
    # level rather than per raft. finer granualarity but more overhead in
    # starting up shifter.
    # QUESTION: which bits can go to sensor level?
    # QUESTION: how is efficiency here compared to losing efficiency by runs having wasted long-tail (wall and cpu) time?
    raft_list_fn = "{repo_dir}/rerun/{rerun}/raft_list_for_visit.{visit_id}".format(repo_dir=configuration.repo_dir, rerun=rerun1, visit_id=visit_id)

    raft_list_future = raft_list_for_visit(
        configuration.repo_dir,
        visit_id,
        raft_list_fn,
        stdout=logdir+raft_list_fn+".stdout",
        stderr=logdir+raft_list_fn+".stderr",
        wrap=configuration.wrap)

    return process_visit_rafts(visit_id, raft_list_fn, inputs=[raft_list_future])


# this must be called with visit_file_future passed into inputs to make it wait for
# the visit_file named in visit_file to be ready. TODO: replace with parsl File based
# futures
@parsl.python_app(executors=['submit-node'], join=True)
def process_visits(visit_file, inputs=None):
    # should make some comment here about how we have to explicitly wait for a
    # result here in the main workflow code, rather than using visit_file_future
    # as a dependency, because its used to generate more tasks (the
    # monadicness I've referred to elsewhere)
    # This means that it isn't, for example, captured in the dependency graph
    # for visualisation, and that there is some constraint on expressing
    # concurrency.

    logger.info("WFLOW: submitting task_calexps")
    visit_lines = read_and_strip(visit_file)

    logger.info("WFLOW:  There were "+str(len(visit_lines))+" visits read from "+str(visit_file))

    nvisits = 0
    visit_futures = []
    for visit_id in visit_lines:

        nvisits += 1

        if int(visit_id) < visit_min or int(visit_id) > visit_max:
            continue

        visit_futures.append(process_visit(visit_id))

        # End of loop over rafts
    logger.info("WFLOW: Finished task definitions for {} visits".format(nvisits))
    return combine(inputs=visit_futures)



###########################
## Sensor/Raft processing
###########################
if doSensor:
    logger.info("WFLOW: Making visit file from raw_visit table")
    visit_file = "{repo_dir}/rerun/{rerun}/all_visits_from_registry.list".format(
        repo_dir=configuration.repo_dir, rerun=rerun1)
    visit_file_future = make_visit_file(
        configuration.repo_dir,
        visit_file,
        stdout=logdir+"make_visit_file.stdout",
        stderr=logdir+"make_visit_file.stderr",
        wrap=configuration.wrap_sql,
        parsl_resource_specification={"priority": (1000,)})

    visits_future = process_visits(visit_file, inputs=[visit_file_future])
    logger.info("WFLOW: Waiting for completion of all sensor/raft oriented tasks")

    # wait for them all to complete ...
    # ... and throw exception here if any of them threw exceptions
    # This is a bottleneck for d/s tasks: a single failure will halt the entire workflow
    visits_future.result()
    logger.info("WFLOW: sensor/raft-oriented tasks complete")

else:
    logger.warning("WFLOW: Skipping sensor/raft level processing")
    pass





#######################################
## Prepare tract/patch processing
#######################################

logger.info("WFLOW: Setup tract/patch processing")

####################################################################################################
##
## 6/18/2020 IMPORTANT NOTES for DC2 Year 1 (partial) processing:
##
##
##   1. The following code has been changed such that it *only* works for tract/patch processing against
##      Y01 data repo, i.e., it will no longer perform sensor/raft processing properly due to changes
##      in the "rerun" naming.

## Override "rerun3" so that it points to the DC2 run 2.2i repo at NERSC
rerun3 = 'run2.2i-calexp-v1-copy'    ## This directory contains all calexp data in $SCRATCH (not CFS)

##
##   2. Another change is the limitation on visitIDs present in the various sql queries.  This is
##      to limit the scope of processing to only the Y01 data (the repo contains much more)

## Define the beginning and ending visitIDs for DC2 Year 1 data   <===== NOW DONE IN THE configuration.py
#vStart = 230
#vEnd = 262622

####################################################################################################

# now we can do coadds. This is concurrent by tract, not by visit.
# information about tracts comes from the result of tract2visit_mapper
# being finished. so we need all tract2visit mappers to be finished in
# order to figure out the parallelisation, and then we need each visit
# that is touched by a tract to be finished in order to actually
# coadd that tract.
# so first lets see if I can have a single barrier here that
# makes all of the above trivially true, to get the co-add calls
# working
# then after that I'd like to try getting the concurrency more fine
# grained

# from johann:

# in order to follow the sequence of job spawning for coaddDriver you need
# to read in that order:

# setup_fullcoadd, which either look at a provided list of tracts in a file,
# or build this list out of all the tracts referenced in the tract_visit
# mapper DB; the it launches one subtask per tract
# (benc: this workflow should generate the DB from the tract_visit mapper DB)

# setup_patch, which looks in the DB for the list of patches that this tract
# has (some tracts can have empty patches, especially in DC2), then it
# subdivides into a small number of patches and launch nested subtasks for
# each of these subset of patches

# setup_coaddDriver, which takes the tract and the patches provided by
# setup_patch, lists all the visits that intersect these patches, compare if
# requested to a provided set of visits (critical to only coadd a given number
# of years for instance), and then launch one final nested subtask for each
# filter. This nested subtask runs coaddDriver.py


@lsst_app2
def make_tract_list(repo_dir, metadata_dir, visit_min, visit_max, tracts_file,
                    stdout=None, stderr=None, wrap=None, parsl_resource_specification=None):
    return wrap('/usr/bin/time -v sqlite3 {metadata_dir}/tracts_mapping.sqlite3 "SELECT DISTINCT tract FROM overlaps where visit >= {visit_min} and visit <= {visit_max} order by tract asc;" > {tracts_file}'.format(metadata_dir=metadata_dir, visit_min=visit_min, visit_max=visit_max, tracts_file=tracts_file))


@lsst_app2
def make_patch_list_for_tract(metadata_dir, tract, visit_min, visit_max, patches_file, stdout=None, stderr=None, wrap=None, parsl_resource_specification=None):
    # this comes from srs/pipe_setups/setup_patch
    return wrap('/usr/bin/time -v sqlite3 "file:{metadata_dir}/tracts_mapping.sqlite3?mode=ro" "SELECT DISTINCT patch FROM overlaps WHERE tract={tract} and visit >= {visit_min} and visit <= {visit_max};" > {patches_file}'.format(metadata_dir=metadata_dir, tract=tract, visit_min=visit_min, visit_max=visit_max, patches_file=patches_file))


tracts_file = "{metadata_dir}/tracts.list".format(repo_dir=configuration.repo_dir, metadata_dir=metadata_dir)

# Extract metadata to drive following DM stack tasks
if doSqlite:
    logger.info("WFLOW: Make tract list")
    tract_list_future = make_tract_list(
        configuration.repo_dir,
        metadata_dir,
        visit_min,
        visit_max,
        tracts_file,
        stdout=logdir+"make_tract_list.stdout",
        stderr=logdir+"make_tract_list.stderr",
        wrap=configuration.wrap)

    logger.info("WFLOW: Awaiting results from make_tract_list")
    tract_list_future.result()
    # we can do this blokcing result call here because
    # there is no more work that can be submitted until
    # we know the tract list

    tract_lines = read_and_strip(tracts_file)

    tract_patch_futures = {}
    for tract_id in tract_lines:
        if tract_subset and not int(tract_id) in tract_subset:
            continue
        logger.info("WFLOW: process tract {}".format(tract_id))

        # assemble a patch list for this tract, as in setup_patch
        patches_file = "{metadata_dir}/patches-for-tract-{tract}.list".format(tract=tract_id, metadata_dir=metadata_dir)
        tract_patch_futures[tract_id] = make_patch_list_for_tract(
            metadata_dir,
            tract_id,
            visit_min,
            visit_max,
            patches_file,
            stdout=logdir+"make_patch_list_for_tract_{}.stdout".format(tract_id),
            stderr=logdir+"make_patch_list_for_tract_{}.stderr".format(tract_id),
            wrap=configuration.wrap_sql)

    # for each tract, for each patch, generate a list of visits that
    # overlap this tract/patch from the tract db - see srs/pipe_setups/?sky_corr

    # so we can generate this list once we have all the patch information, even
    # if the final image processing steps haven't happened for a particular visit;
    # as long as we then somehow # depend on the processing for that visit
    # completing before we do the coadd.

    # doing this as a separate loop from the above loop rather than doing something useful with dependencies is ugly.

else:
    logger.info("Skipping SQLite3 block")
    tract_lines = read_and_strip(tracts_file)
    tract_patch_futures = {}
    # End of doSqlite block


@lsst_app2
def visits_for_tract_patch_filter(metadata_dir, tract_id, patch_id,
                                  filter_id, visit_min, visit_max, visit_file,
                                  stdout=None, stderr=None, wrap=None,
                                  parsl_resource_specification=None):
    # TODO: set_coaddDriver treats filter_id differently here:
    # it takes a *list* of filters not a single filter, and generates
    # SQL from that somehow. Ask Johann about it? Is there some
    # non-trivial interaction of multiple filters here?
    sql = "SELECT DISTINCT visit FROM overlaps WHERE tract={tract_id} AND filter='{filter_id}' AND patch=\'{patch_id}\' and visit >= {visit_min} and visit <= {visit_max};".format(tract_id=tract_id, patch_id=patch_id, filter_id=filter_id, visit_min=visit_min, visit_max=visit_max)
    # sqlite returns a list of visitIDs, one per line.  This needs to be converted
    # into a single line of the form:
    #     --selectID visit=<visitID1>^<visitID2>^...
    return wrap('/usr/bin/time -v sqlite3 "file:{metadata_dir}/tracts_mapping.sqlite3?mode=ro" "{sql}" > {visit_file} ; cat {visit_file}  | tr \'\\n\' \'^\' | sed s\'/.$//\' | sed \'s/^/--selectId visit=/\' > {visit_file}.selectid'.format(metadata_dir=metadata_dir, tract_id=tract_id, patch_id=patch_id, filter_id=filter_id, sql=sql, visit_file=visit_file))


tract_patch_visit_futures = []
ntracts = 0
npatches = 0

if tract_subset:
    logger.warning("WFLOW: Processing only selected tracts: "+str(tract_subset))


@parsl.python_app(executors=['submit-node'], join=True)
def process_patches(tract_id, patches_file, inputs=None):
    global npatches
    # TODO: this idiom of reading and stripping is used in a few places
    #   - factor it
    # something like:   for stripped_lines_in_file("filename"):
    # for direct reading from file - where it returns a list...
    patch_lines = read_and_strip(patches_file)

    nplines = len(patch_lines)
    logger.info("WFLOW: tract {} contains {} patches".format(tract_id, nplines))

    this_tract_all_patches_futures = []
    npatches_per_tract = 0
    for patch_id in patch_lines:
        npatches += 1
        npatches_per_tract += 1
        # patch_id form used for sqlite queries, e.g., "(4, 1)"
        patch_idx = re.sub("[() ]", "", patch_id)  # This form used for DM stack tools, e.g., "4,1"
        patch_idl = re.sub(",", "-", patch_idx)    # This form used for log files, e.g., "4-1"
        if patch_subset and patch_idl not in patch_subset:  # favoured patch handling, for testing
            continue

        logger.info("WFLOW: setup coadd processing for tract {} patch {}".format(tract_id, patch_idx))

        this_patch_futures = []

        for filter_id in ["g", "r", "i", "z", "y", "u"]:
            logger.info("WFLOW: generating visit list for tract {} patch {} filter {}".format(tract_id, patch_id, filter_id))

            # remove shell-fussy characters for filename. this avoids shell escaping. be careful that this still generates unique filenames.
            filename_patch_id = patch_id.replace(" ", "-").replace("(", "").replace(")", "").replace(",", "")

            visit_file = "{metadata_dir}/visits-for-tract-{tract_id}-patch-{filename_patch_id}-filter-{filter_id}.list".format(metadata_dir=metadata_dir, tract_id=tract_id, patch_id=patch_id, filename_patch_id=filename_patch_id, filter_id=filter_id)

            # Slight variation depending on whether the sqlite query results have already been produced
            if doSqlite:
                fut = visits_for_tract_patch_filter(metadata_dir,
                                                    tract_id,
                                                    patch_id,
                                                    filter_id,
                                                    visit_min,
                                                    visit_max,
                                                    visit_file,
                                                    stdout=logdir+"visit_for_tract_{}_patch_{}_filter_{}.stdout".format(tract_id, patch_id, filter_id),
                                                    stderr=logdir+"visit_for_tract_{}_patch_{}_filter_{}.stderr".format(tract_id, patch_id, filter_id),
                                                    wrap=configuration.wrap_sql)
                # TODO: this visit_file should become an input/output File
                # object to give the dependency instead of relying on
                # 'fut'

                # the visit_file is sometimes empty - we could optimise
                # away a singularity+coadd driver launch by only
                # submitting that task if the file isn't empty (see
                # monadic behaviour: but Maybe style do/don't, rather than
                # []-style "how many?")
                iList = [fut]
            else:
                iList = []

            fut2 = tracts.coadd_parsl_driver(configuration,
                                             rerun3,
                                             rerun4,
                                             tract_id,
                                             patch_idx,
                                             filter_id,
                                             visit_file,
                                             None,
                                             inputs=iList,
                                             logbase=logdir+"coadd_for_tract_{}_patch_{}_filter_{}".format(tract_id, patch_idl, filter_id),
                                             wrap=configuration.wrap)
            # now we have a load of files like this:
            #   visits-for-tract-4232-patch-6,-4-filter-g.list
            # so for each of those files, launch coadd for this
            # tract/patch/filter

            # filt=u has different processing here that i'm not sure
            # why... looks like stuff goes into a different rerun out
            # directory. in workflows/srs/pipe_setups/run_coaddDrive -
            # TODO: ask johann what the reasoning for that is.  I want
            # to try do different stuff with rerun directories anyway.

            #    coaddDriver.py ${OUT_DIR} --rerun ${RERUN1}:${RERUN2}-grizy --id tract=${TRACT} patch=${PATCH} filter=$FILT @${visit_file} --cores $((NSLOTS+1)) --doraise --longlog

            this_patch_futures.append(fut2)
            pass

        logger.info("WFLOW: setup multiband processing for tract {} patch {}".format(tract_id, patch_idx))
        fut3 = tracts.multiband_parsl_driver(configuration,
                                             rerun4,
                                             rerun5,
                                             tract_id,
                                             patch_idx,
                                             ["u", "g", "r", "i", "z", "y"],
                                             inputs=this_patch_futures,
                                             logbase=logdir+"multiband_for_tract_{}_patch_{}".format(tract_id, patch_idl),
                                             wrap=configuration.wrap)

        this_tract_all_patches_futures.append(fut3)
        pass
    
    return combine(inputs=this_tract_all_patches_futures)
    ### end of process_patches()



    
##############################
## Tract/Patch processing
##############################
for tract_id in tract_lines:

    if tract_subset and not int(tract_id) in tract_subset:
        continue

    ntracts += 1
    logger.info("WFLOW: generating patch list for tract {}".format(tract_id))

    # TODO: this filename should be coming from a File output object from
    # the earlier futures, and not hardcoded here and in patch list generator.
    patches_file = "{metadata_dir}/patches-for-tract-{tract}.list".format(tract=tract_id, repo_dir=configuration.repo_dir, metadata_dir=metadata_dir)

    tract_patch_list_futures = []
    if tract_id in tract_patch_futures.keys():
        logger.info("WFLOW: waiting for patches list for tract {} to be available".format(tract_id))
        tract_patch_list_futures.append(tract_patch_futures[tract_id])
    else:
        logger.info("WFLOW: assuming patch list for tract {} has been generated some other way".format(tract_id))
        # need to do this block before we can do a read...
        # but actually that means this body should move into its own local

    logger.info("WFLOW: begin processing patches for tract {}".format(tract_id))
    patches_fut = process_patches(tract_id, patches_file, inputs=tract_patch_list_futures)

    terminal_futures.append(patches_fut)
    # johann: setup_coaddDriver, which takes the tract and the patches
    # provided by setup_patch, lists all the visits that intersect these
    # patches, compare if requested to a provided set of visits
    # (critical to only coadd a given number of years for instance),
    # and then launch one final nested subtask for each filter.
    # This nested subtask runs coaddDriver.py
    pass


########################
## Finale
########################
logger.info("WFLOW: Awaiting results from terminal_futures")
logger.info("WFLOW: #tracts = "+str(ntracts)+", #patches = "+str(npatches))
concurrent.futures.wait(terminal_futures)
[future.result() for future in terminal_futures]
logger.info("WFLOW: Reached the end of the parsl driver for DM pipeline")