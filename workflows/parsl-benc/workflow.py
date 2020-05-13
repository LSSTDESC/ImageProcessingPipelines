#!/usr/bin/env python
## workflow.py - Main Parsl script for DESC DRP workflow

## To run:
# initial conda setup on cori:
# $ ./initialize/initConda.sh

# to run the workflow, assuming above setup has been done:
# $ ./runWorkflow.sh CONFIG_FILE_NAME

import concurrent.futures
import functools
import logging

import parsl
from parsl import bash_app

import checkpointutil  # noqa: F401 - for import-time checkpoint config
import configuration
import ingest

##### FLAGS ######
doIngest = False     # skip the ingest step (if repo already in place)


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


rerun1_name = "R1-1"  # contains outputs of: ingest and skymap
rerun2_name = "R2-1"  # contains outputs of: singleFrameDriver
rerun3_name = "R3-3"  # ... etc
rerun4_name = "R4-1"
rerun5_name = "R5-1"

rerun1 = rerun1_name
rerun2 = rerun1 + "." + rerun2_name
rerun3 = rerun2 + "." + rerun3_name
rerun4 = rerun3 + "." + rerun4_name
rerun5 = rerun4 + "." + rerun5_name


logger = logging.getLogger("parsl.dm")

parsl.set_stream_logger()

logger.info("Parsl driver for DM pipeline")

configuration = configuration.load_configuration()

parsl.load(configuration.parsl_config)

# tell wrapper about parsl run_dir which isn't decided until
# after parsl.load()
configuration.wrap = functools.partial(configuration.wrap,
                                       run_dir=parsl.dfk().run_dir)

logdir = parsl.dfk().run_dir + "/dm-logs/"
logger.info("Log directory is " + logdir)


# This is a list of futures which should be waited on at the end
# of the workflow before exiting (rather than being intermediate
# results used by subsequent steps)
terminal_futures = []


## INGEST
# (old) ingest_future = ingest.perform_ingest(configuration)
# (new) ingest_future = ingest.perform_ingest(configuration, logdir)
# logger.info("waiting for ingest(s) to complete")
# ingest_future.result()
# logger.info("ingest(s) completed")

if doIngest:
    ingest_future = ingest.perform_ingest(configuration, logdir, rerun1)
else:
    logger.info("Skip ingest")
    pass


# This defines a decorator lsst_app which captures the options that
# most of the core application code will need
lsst_app = bash_app(executors=["batch-1"],
                    cache=True,
                    ignore_for_checkpointing=["stdout", "stderr", "wrap"])

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
###OLD## @bash_app(executors=["batch-1"], cache=True, ignore_for_checkpointing=["stdout", "stderr", "wrap"])
@lsst_app
def make_sky_map(repo_dir, rerun, stdout=None, stderr=None, wrap=None):
    return wrap("makeSkyMap.py {} --rerun {}".format(repo_dir, rerun))


# TODO: this can run in parallel with ingest
logger.info("launching makeSkyMap")
skymap_future = make_sky_map(configuration.repo_dir, rerun1,
                             stdout=logdir+"make_sky_map.stdout",
                             stderr=logdir+"make_sky_map.stderr",
                             wrap=configuration.wrap)


if doIngest:
    logger.info("waiting for ingest(s) to complete")
    ingest_future.result()
    logger.info("ingest(s) completed")
    pass

#  setup_calexp: use DB to make a visit file
logger.info("Making visit file from raw_visit table")


# TODO: this is sql so should use the sqlwrapper
@bash_app(executors=["batch-1"], cache=True,
          ignore_for_checkpointing=["stdout", "stderr", "wrap"])
def make_visit_file(repo_dir, visit_file, stdout=None, stderr=None, wrap=None):
    return wrap(('sqlite3 {repo_dir}/registry.sqlite3 '
                 '"SELECT DISTINCT visit FROM raw_visit;" '
                 '> {visit_file}').format(repo_dir=repo_dir,
                                          visit_file=visit_file))


visit_file = "{repo_dir}/rerun/{rerun}/all_visits_from_registry.list".format(
    repo_dir=configuration.repo_dir, rerun=rerun1)
visit_file_future = make_visit_file(
    configuration.repo_dir,
    visit_file,
    stdout=logdir+"make_visit_file.stdout",
    stderr=logdir+"make_visit_file.stderr",
    wrap=configuration.wrap)

logger.info("Waiting for visit list generation to complete")
visit_file_future.result()
logger.info("Visit list generation completed")
# should make some comment here about how we have to explicitly wait for a
# result here in the main workflow code, rather than using visit_file_future
# as a dependency, because its used to generate more tasks (the
# monadicness I've referred to elsewhere)
# This means that it isn't, for example, captured in the dependency graph
# for visualisation, and that there is some constraint on expressing
# concurrency.

logger.info("waiting for makeSkyMap to complete")
skymap_future.result()
logger.info("makeSkyMap completed")

logger.info("submitting task_calexps")


@lsst_app
def single_frame_driver(repo_dir, rerun, visit_id, raft_name,
                        stdout=None, stderr=None, wrap=None):
    # params for stream are WORKDIR=workdir, VISIT=visit_id
    # this is going to be something like found in
    # workflows/srs/pipe_setups/run_calexp
    # run_calexp uses --cores as NSLOTS+1. I'm using cores 1 because I
    # am not sure of the right parallelism here.

    return wrap(("singleFrameDriver.py --batch-type none {repo_dir} "
                 "--rerun {rerun} "
                 "--id visit={visit} raftName={raft_name} "
                 "--calib {repo_dir}/CALIB/ "
                 "--clobber-versions --cores 1 --timeout 999999999 "
                 "--loglevel CameraMapper=warn").format(repo_dir=repo_dir,
                                                        rerun=rerun,
                                                        visit=visit_id,
                                                        raft_name=raft_name))


@bash_app(executors=["batch-1"], cache=True,
          ignore_for_checkpointing=["stdout", "stderr", "wrap"])
def raft_list_for_visit(repo_dir, visit_id, out_filename,
                        stderr=None, stdout=None, wrap=None):
    return wrap(("sqlite3 {repo_dir}/registry.sqlite3 "
                 "'SELECT DISTINCT raftName FROM raw WHERE visit={visit_id}' "
                 "> {out_filename}").format(repo_dir=repo_dir,
                                            visit_id=visit_id,
                                            out_filename=out_filename))


# the parsl checkpointing for this won't detect if we ingested more stuff
# to do with the specified visit - I'm not sure quite the right way to do
# it, and I think its only useful in during workflow development when the
# original ingest list might change? would need eg "files in each visit"
# list to generate a per-visit input "version" id/hash
@lsst_app
def check_ccd_astrometry(root_softs, repo_dir, rerun, visit, inputs=[],
                         stderr=None, stdout=None, wrap=None):
    # inputs=[] ignored but used for dependency handling
    return wrap("{root_softs}/ImageProcessingPipelines/python/util/checkCcdAstrometry.py {repo_dir}/rerun/{rerun} --id visit={visit} --loglevel CameraMapper=warn".format(visit=visit, rerun=rerun, repo_dir=repo_dir, root_softs=root_softs))

# the parsl checkpointing for this won't detect if we ingested more stuff
# to do with the specified visit - see comments for check_ccd_astrometry
@lsst_app
def tract2visit_mapper(root_softs, repo_dir, rerun, visit, inputs=[],
                       stderr=None, stdout=None, wrap=None):
    # TODO: this seems to be how $REGISTRIES is figured out (via $WORKDIR)
    # perhaps? I'm unsure though
    registries = "{repo_dir}/rerun/{rerun}".format(repo_dir=repo_dir,
                                                   rerun=rerun)

    # the srs workflow has a separate output database per visit, which is
    # elsewhere merged into a single DB. That's awkward... there's probably
    # a reason to do with concurrency or shared fs that needs digging into.
    return wrap("mkdir -p {registries} && {root_softs}/ImageProcessingPipelines/python/util/tract2visit_mapper.py --indir={repo_dir}/rerun/{rerun} --db={registries}/tracts_mapping.sqlite3 --visits={visit}".format(repo_dir=repo_dir, rerun=rerun, visit=visit, registries=registries, root_softs=root_softs))


@lsst_app
def sky_correction(repo_dir, rerun, visit, raft_name, inputs=[], stdout=None, stderr=None, wrap=None):
    return wrap("skyCorrection.py {repo_dir}  --rerun {rerun} --id visit={visit} raftName={raft_name} --batch-type none --cores 1  --calib {repo_dir}/CALIB/ --timeout 999999999 --no-versions --loglevel CameraMapper=warn".format(repo_dir=repo_dir, rerun=rerun, visit=visit, raft_name=raft_name))


##########################################################################
##########################################################################


with open(visit_file) as f:
    visit_lines = f.readlines()

visit_futures = []
for (n, visit_id_unstripped) in zip(range(0, len(visit_lines)), visit_lines):

    ################################################################
    if n > 100: break     ## DEBUG: limit number of visits processed
    ################################################################

    visit_id = visit_id_unstripped.strip()
    logger.info("=> Begin processing visit "+str(visit_id))


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

    raft_list_future.result()
    # this wait here means that we don't get parallelisation so much
    # there are problems with launching tasks within tasks due to locking up
    # a local worker... so avoid doing that.
    # i.e. the monadness

    with open(raft_list_fn) as f:
        raft_lines = f.readlines()
        pass
    rlist = [x.strip() for x in raft_lines]
    logger.info("=> There are "+str(len(rlist))+ " rafts to process:")
    logger.info(str(rlist))

    this_visit_single_frame_futs = []

    for (m, raft_name_stripped) in zip(range(0, len(raft_lines)), raft_lines):
        raft_name = raft_name_stripped.strip()
        logger.info("visit {} raft {}".format(visit_id, raft_name))

        # this call is based on run_calexp shell script
        # assume visit_id really is a visit id... workflows/srs/pipe_setups/setup_calexp has a case where the visit file has two fields per line, and this is handled differently there. I have ignored that here.
        # raft_name is the $RAFTNAME environment variable in run_calexp in the XML workflows
        sfd_output_basename = "single_frame_driver.visit-{}.raft-{}".format(visit_id, raft_name)
        this_raft_single_frame_fut = single_frame_driver(
            configuration.repo_dir,
            rerun1 + ":" + rerun2,
            visit_id,
            raft_name,
            stdout=logdir+sfd_output_basename+".stdout",
            stderr=logdir+sfd_output_basename+".stderr",
            wrap=configuration.wrap)
        # this is invoked in run_calexp with $OUT_DIR at the first parameter, but that's not something
        # i've used so far -- so I'm using IN_DIR as used in previous steps
        # TODO: is that the right thing to do? otherwise how does IN_DIR and OUT_DIR differ?
        sky_correction_stdbase = "sky_correction.visit-{}.raft-{}".format(visit_id, raft_name)
        this_visit_single_frame_futs.append(sky_correction(
            configuration.repo_dir,
            rerun2 + ":" + rerun3,
            visit_id,
            raft_name,
            inputs=[this_raft_single_frame_fut],
            stdout=logdir+sky_correction_stdbase+".stdout",
            stderr=logdir+sky_correction_stdbase+".stderr",
            wrap=configuration.wrap))

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
    fut_check_ccd = check_ccd_astrometry(
        configuration.root_softs,
        configuration.repo_dir,
        rerun3,
        visit_id,
        inputs=this_visit_single_frame_futs,
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
        configuration.root_softs,
        configuration.repo_dir,
        rerun3,
        visit_id,
        inputs=this_visit_single_frame_futs,
        stdout=logdir+tract2visit_mapper_stdbase+".stdout",
        stderr=logdir+tract2visit_mapper_stdbase+".stderr",
        wrap=configuration.wrap)

    # This could go into terminal futures or we could explicitly wait for it
    # here. By waiting for it here, we ensure that the check has passed
    # before doing anything with the information.
    # By not waiting for it, we increase parallelism, but we might encounter
    # a downstream problem of some kind before discovering check_ccd is
    # broken?
    terminal_futures.append(fut_check_ccd)

    visit_futures.append(fut_tract2visit)

    # TODO: visitAnlysis.py for stream and visit - this involves sqlite


logger.info("Waiting for completion of all per-visit tasks")

# wait for them all to complete ...
concurrent.futures.wait(visit_futures)

# ... and throw exception here if any of them threw exceptions
[future.result() for future in visit_futures]

logger.info("Begin processing tracts")

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


@bash_app(executors=["batch-1"], cache=True,  ignore_for_checkpointing=["stdout", "stderr", "wrap"])
def make_tract_list(repo_dir, rerun, tracts_file, stdout=None, stderr=None, wrap=None):
    # this comes from srs/pipe_setups/setup_fullcoadd
    return wrap('sqlite3 {repo_dir}/rerun/{rerun}/tracts_mapping.sqlite3 "SELECT DISTINCT tract FROM overlaps;" > {tracts_file}'.format(repo_dir=repo_dir, rerun=rerun, tracts_file=tracts_file))


@bash_app(executors=["batch-1"], cache=True,  ignore_for_checkpointing=["stdout", "stderr", "wrap"])
def make_patch_list_for_tract(repo_dir, rerun, tract, patches_file, stdout=None, stderr=None, wrap=None):
    # this comes from srs/pipe_setups/setup_patch
    return wrap('sqlite3 {repo_dir}/rerun/{rerun}/tracts_mapping.sqlite3 "SELECT DISTINCT patch FROM overlaps WHERE tract={tract};" > {patches_file}'.format(repo_dir=repo_dir, rerun=rerun, tract=tract, patches_file=patches_file))

#    sqlite3 ${OUT_DIR}/rerun/${RERUN1}/tracts_mapping.sqlite3 "select DISTINCT tract from overlaps;" > ${WORKDIR}/all_tracts.list
#    registries = "{repo_dir}/rerun/{rerun}/registries".format(repo_dir=repo_dir, rerun=rerun)
#    return wrap("mkdir -p {registries} && {root_softs}/ImageProcessingPipelines/python/util/tract2visit_mapper.py --indir={repo_dir}/rerun/{rerun} --db={registries}/tracts_mapping_{visit}.sqlite3

logger.info("Create tract list")
tracts_file = "{repo_dir}/rerun/{rerun}/tracts.list".format(repo_dir=configuration.repo_dir, rerun=rerun3)
tract_list_future = make_tract_list(
    configuration.repo_dir,
    rerun3,
    tracts_file,
    stdout=logdir+"make_tract_list.stdout",
    stderr=logdir+"make_tract_list.stderr",
    wrap=configuration.wrap)

tract_list_future.result()

with open(tracts_file) as f:
    tract_lines = f.readlines()

tract_patch_futures = []
for tract_id_unstripped in tract_lines:
    tract_id = tract_id_unstripped.strip()
    logger.info("process tract {}".format(tract_id))

    # assemble a patch list for this tract, as in setup_patch
    patches_file = "{repo_dir}/rerun/{rerun}/patches-for-tract-{tract}.list".format(tract=tract_id, repo_dir=configuration.repo_dir, rerun=rerun3)
    tract_patch_futures.append(make_patch_list_for_tract(
        configuration.repo_dir,
        rerun3,
        tract_id,
        patches_file,
        stdout=logdir+"make_patch_list_for_tract_{}.stdout".format(tract_id),
        stderr=logdir+"make_patch_list_for_tract_{}.stderr".format(tract_id),
        wrap=configuration.wrap_sql))

# instead of this wait(), make downstream depend on File objects that
# are the patch list.
concurrent.futures.wait(tract_patch_futures)

# for each tract, for each patch, generate a list of visits that
# overlap this tract/patch from the tract db - see srs/pipe_setups/?sky_corr

# so we can generate this list once we have all the patch information, even
# if the final image processing steps haven't happened for a particular visit;
# as long as we then somehow # depend on the processing for that visit
# completing before we do the coadd.

# doing this as a separate loop from the above loop rather than doing something useful with dependencies is ugly.

@bash_app(executors=["batch-1"], cache=True,  ignore_for_checkpointing=["stdout", "stderr", "wrap"])
def visits_for_tract_patch_filter(repo_dir, rerun, tract_id, patch_id, filter_id, visit_file, stdout=None, stderr=None, wrap=None):
    # TODO: set_coaddDriver treats filter_id differently here: it takes a *list* of filters not a
    # single filter, and generates SQL from that somehow. Ask Johann about it? Is there some
    # non-trivial interaction of multiple filters here?
    sql = "SELECT DISTINCT visit FROM overlaps WHERE tract={tract_id} AND filter='{filter_id}' AND patch=\'{patch_id}\';".format(repo_dir=repo_dir, rerun=rerun, tract_id=tract_id, patch_id=patch_id, filter_id=filter_id)
    return wrap('sqlite3 {repo_dir}/rerun/{rerun}/tracts_mapping.sqlite3 "{sql}" > {visit_file}'.format(repo_dir=repo_dir, rerun=rerun, tract_id=tract_id, patch_id=patch_id, filter_id=filter_id, sql=sql, visit_file=visit_file))




@bash_app(executors=["batch-1"], cache=True,  ignore_for_checkpointing=["stdout", "stderr", "wrap"])
def coadd_driver(repo_dir, rerun, tract_id, patch_id, filter_id, visit_file, inputs=None, stdout=None, stderr=None, wrap=None):
    # TODO: what does --doraise mean?
    return wrap("coaddDriver.py {repo_dir} --rerun {rerun} --id tract={tract_id} patch='{patch_id}' filter={filter_id} @{visit_file} --cores 1 --batch-type none --doraise --longlog".format(repo_dir=repo_dir, rerun=rerun, tract_id=tract_id, patch_id=patch_id, filter_id=filter_id, visit_file=visit_file))


@bash_app(executors=["batch-1"], cache=True,  ignore_for_checkpointing=["stdout", "stderr", "wrap"])
def multiBand_driver(repo_dir, rerun, tract_id, patch_id, inputs=[], stdout=None, stderr=None, wrap=None):
    return wrap("multiBandDriver.py {repo_dir} --rerun {rerun} --id tract={tract_id} patch='{patch_id}' filter=u,g,r,i,z,y --cores 1 --batch-type none --doraise --longlog".format(repo_dir=repo_dir, rerun=rerun, tract_id=tract_id, patch_id=patch_id))


tract_patch_visit_futures = []
for tract_id_unstripped in tract_lines:
    tract_id = tract_id_unstripped.strip()

    logger.info("generating visit list for patches in tract {}".format(tract_id))

    # TODO: this filename should be coming from a File output object from
    # the earlier futures, and not hardcoded here and in patch list generator.
    patches_file = "{repo_dir}/rerun/{rerun}/patches-for-tract-{tract}.list".format(tract=tract_id, repo_dir=configuration.repo_dir, rerun=rerun3)

    # TODO: this idiom of reading and stripping is used in a few places
    #   - factor it
    # something like:   for stripped_lines_in_file("filename"):
    # for direct reading from file - where it returns a list...
    with open(patches_file) as f:
        patch_lines = f.readlines()

    for patch_id_unstripped in patch_lines:
        patch_id = patch_id_unstripped.strip()
        logger.info("generating visit list for tract {} patch {}".format(tract_id, patch_id))

        this_patch_futures = []

        for filter_id in ["g", "r", "i", "z", "y", "u"]:
            logger.info("generating visit list for tract {} patch {} filter {}".format(tract_id, patch_id, filter_id))

            filename_patch_id = patch_id.replace(" ", "-").replace("(", "").replace(")", "")  # remove shell-fussy characters for filename. this avoids shell escaping. be careful that this still generates unique filenames.
            visit_file = "{repo_dir}/rerun/{rerun}/visits-for-tract-{tract_id}-patch-{filename_patch_id}-filter-{filter_id}.list".format(repo_dir=configuration.repo_dir, rerun=rerun3, tract_id=tract_id, patch_id=patch_id, filename_patch_id=filename_patch_id, filter_id=filter_id)
            fut = visits_for_tract_patch_filter(configuration.repo_dir, rerun3, tract_id, patch_id, filter_id,
                                                visit_file,
                                                stdout=logdir+"visit_for_tract_{}_patch_{}_filter_{}.stdout".format(tract_id, patch_id, filter_id),
                                                stderr=logdir+"visit_for_tract_{}_patch_{}_filter_{}.stderr".format(tract_id, patch_id, filter_id),
                                                wrap=configuration.wrap_sql)
            # TODO: this visit_file should become an input/output File object to give the dependency instead of relying on 'fut'

            # the visit_file is sometimes empty - we could optimise away a singularity+coadd driver launch by only submitting that task if the file isn't empty (see monadic behaviour: but Maybe style do/don't, rather than []-style "how many?")
            fut2 = coadd_driver(configuration.repo_dir, rerun3 + ":" + rerun4, tract_id, patch_id, filter_id, visit_file, inputs=[fut],
                                stdout=logdir+"coadd_for_tract_{}_patch_{}_filter_{}.stdout".format(tract_id, patch_id, filter_id),
                                stderr=logdir+"coadd_for_tract_{}_patch_{}_filter_{}.stderr".format(tract_id, patch_id, filter_id),
                                wrap=configuration.wrap)
            # now we have a load of files like this:
            #   visits-for-tract-4232-patch-6,-4-filter-g.list
            # so for each of those files, launch coadd for this
            # tract/patch/filter

            # filt=u has different processing here that i'm not sure why... looks like stuff goes into a different rerun out directory. in workflows/srs/pipe_setups/run_coaddDrive - TODO: ask johann what the reasoning for that is.  I want to try do different stuff with rerun directories anyway.

            #    coaddDriver.py ${OUT_DIR} --rerun ${RERUN1}:${RERUN2}-grizy --id tract=${TRACT} patch=${PATCH} filter=$FILT @${visit_file} --cores $((NSLOTS+1)) --doraise --longlog

            this_patch_futures.append(fut2)

        fut3 = multiBand_driver(configuration.repo_dir, rerun4 + ":" + rerun5, tract_id, patch_id, inputs=this_patch_futures,
                                stdout=logdir+"multiband_for_tract_{}_patch_{}.stdout".format(tract_id, patch_id),
                                stderr=logdir+"multiband_for_tract_{}_patch_{}.stderr".format(tract_id, patch_id),
                                wrap=configuration.wrap)

        tract_patch_visit_futures.append(fut3)

        # this query is *per filter* which is another dimension of
        # concurrency but also perhaps another dimension of presence of data?

        # from setup coadd:
        # visit_list=`sqlite3 ${OUT_DIR}/rerun/${RERUN1}/tracts_mapping.sqlite3
        # "SELECT DISTINCT visit FROM overlaps WHERE tract=${TRACT} and
        #     filter='${FILT}' and ${patch_str}"`

    # johann: setup_coaddDriver, which takes the tract and the patches
    # provided by setup_patch, lists all the visits that intersect these
    # patches, compare if requested to a provided set of visits
    # (critical to only coadd a given number of years for instance),
    # and then launch one final nested subtask for each filter.
    # This nested subtask runs coaddDriver.py

terminal_futures += tract_patch_visit_futures
concurrent.futures.wait(terminal_futures)
[future.result() for future in terminal_futures]


logger.info("Reached the end of the parsl driver for DM pipeline")
