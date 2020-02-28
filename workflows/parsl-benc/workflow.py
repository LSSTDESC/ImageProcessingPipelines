#!/usr/bin/env python
import concurrent.futures
import functools
import logging

import parsl
from parsl import bash_app

import checkpointutil  # noqa: F401 - for import-time checkpoint config
import configuration
import ingest



# initial conda setup on cori:
# $ ./initialize/initConda.sh

# to run the workflow, assuming above setup has been done:

# $ ./runWorkflow.sh CONFIG_FILE_NAME

logger = logging.getLogger("parsl.dm")

parsl.set_stream_logger()

logger.info("Parsl driver for DM pipeline")

configuration = configuration.load_configuration()

parsl.load(configuration.parsl_config)

# tell wrapper about parsl run_dir which isn't decided until
# after parsl.load()
configuration.wrap = functools.partial(configuration.wrap, run_dir=parsl.dfk().run_dir)

logdir = parsl.dfk().run_dir + "/dm-logs/"
logger.info("Log directory is " + logdir)

ingest_future = ingest.perform_ingest(configuration, logdir)

logger.info("waiting for ingest(s) to complete")
ingest_future.result()
logger.info("ingest(s) completed")


# now equivalent of DC2DM_2_SINGLEFRAME_NERSC.xml

# setup_calexp .... eg workflows/srs/pipe_setups/setup_calexp

#   makeSkyMap.py
#   QUESTION: in xml, this does copying of files out of one rerun dir into another, neither of which is the rerun dir passed to makeSkyMap... what is going on there? I'm going to ignore reruns entirely here if i can...

# QUESTION: what is the concurrency between make_sky_map and the raw visit list? can they run concurrently or must make_sky_map run before generating the raw visit list?

# ingest list is passed in but not used explicity because it represents that some stuff
# has gone into the DB potentially during ingest - for checkpointing
@bash_app(executors=["worker-nodes"], cache=True, ignore_for_checkpointing=["stdout", "stderr", "wrap"])
def make_sky_map(in_dir, rerun, stdout=None, stderr=None, wrap=None):
    return wrap("makeSkyMap.py {} --rerun {}".format(in_dir, rerun))


# TODO: this can run in parallel with ingest
logger.info("launching makeSkyMap")
rerun = configuration.rerun
skymap_future = make_sky_map(configuration.in_dir, rerun, stdout=logdir+"make_sky_map.stdout", stderr=logdir+"make_sky_map.stderr", wrap=configuration.wrap)
skymap_future.result()
logger.info("makeSkyMap completed")

#  setup_calexp: use DB to make a visit file
logger.info("Making visit file from raw_visit table")


@bash_app(executors=["worker-nodes"], cache=True,  ignore_for_checkpointing=["stdout", "stderr", "wrap"])
def make_visit_file(in_dir, stdout=None, stderr=None, wrap=None):
    return wrap('sqlite3 {}/registry.sqlite3 "select DISTINCT visit from raw_visit;" > all_visits_from_register.list'.format(in_dir))


visit_file_future = make_visit_file(
    configuration.in_dir,
    stdout=logdir+"make_visit_file.stdout",
    stderr=logdir+"make_visit_file.stderr",
    wrap=configuration.wrap)

visit_file_future.result()
# should make some comment here about how we have to explicitly wait for a
# result here in the main workflow code, rather than using visit_file_future
# as a dependency, because its used to generate more tasks (the
# monadicness I've referred to elsewhere)
# This means that it isn't, for example, captured in the dependency graph
# for visualisation, and that there is some constraint on expressing
# concurrency.

logger.info("Finished making visit file")

logger.info("submitting task_calexps")


@bash_app(executors=["worker-nodes"], cache=True,  ignore_for_checkpointing=["stdout", "stderr", "wrap"])
def single_frame_driver(in_dir, rerun, visit_id, raft_name, stdout=None, stderr=None, wrap=None):
    # params for stream are WORKDIR=workdir, VISIT=visit_id
    # this is going to be something like found in workflows/srs/pipe_setups/run_calexp
    # run_calexp uses --cores as NSLOTS+1. I'm using cores 1 because I am not sure of
    # the right parallelism here.
#    return wrap("singleFrameDriver.py --batch-type none {in_dir} --rerun {rerun} --id visit={visit} raftName={raft_name} --cores 1 --timeout 999999999 --loglevel CameraMapper=warn".format(in_dir=in_dir, rerun=rerun, visit=visit_id, raft_name=raft_name))


    return wrap("singleFrameDriver.py --batch-type none {in_dir} --rerun {rerun} --id visit={visit} raftName={raft_name} --clobber-versions --cores 1 --timeout 999999999 --loglevel CameraMapper=warn".format(in_dir=in_dir, rerun=rerun, visit=visit_id, raft_name=raft_name))


@bash_app(executors=["worker-nodes"], cache=True,  ignore_for_checkpointing=["stdout", "stderr", "wrap"])
def raft_list_for_visit(in_dir, visit_id, out_filename, stderr=None, stdout=None, wrap=None):
    return wrap("sqlite3 {in_dir}/registry.sqlite3 'select distinct raftName from raw where visit={visit_id}' > {out_filename}".format(in_dir=in_dir, visit_id=visit_id, out_filename=out_filename))


# the parsl checkpointing for this won't detect if we ingested more stuff to do with the
# specified visit - I'm not sure quite the right way to do it, and I think its only
# useful in during workflow development when the original ingest list might change?
# would need eg "files in each visit" list to generate a per-visit input "version" id/hash
@bash_app(executors=["worker-nodes"], cache=True,  ignore_for_checkpointing=["stdout", "stderr", "wrap"])
def check_ccd_astrometry(root_softs, in_dir, rerun, visit, inputs=[], stderr=None, stdout=None, wrap=None):
    # inputs=[] ignored but used for dependency handling
    return wrap("{root_softs}/ImageProcessingPipelines/python/util/checkCcdAstrometry.py {in_dir}/rerun/{rerun} --id visit={visit} --loglevel CameraMapper=warn".format(visit=visit, rerun=rerun, in_dir=in_dir, root_softs=root_softs))

# the parsl checkpointing for this won't detect if we ingested more stuff to do with the
# specified visit - see comments for check_ccd_astrometry
@bash_app(executors=["worker-nodes"], cache=True,  ignore_for_checkpointing=["stdout", "stderr", "wrap"])
def tract2visit_mapper(root_softs, in_dir, rerun, visit, inputs=[], stderr=None, stdout=None, wrap=None):
    # TODO: this seems to be how $REGISTRIES is figured out (via $WORKDIR) perhaps?
    # I'm unsure though
    registries = "{in_dir}/rerun/{rerun}".format(in_dir=in_dir, rerun=rerun)

    # the srs workflow has a separate output database per visit, which is elsewhere merged into a single DB. That's awkward... there's probably a reason to do with concurrency or shared fs that needs digging into.
    return wrap("mkdir -p {registries} && {root_softs}/ImageProcessingPipelines/python/util/tract2visit_mapper.py --indir={in_dir}/rerun/{rerun} --db={registries}/tracts_mapping.sqlite3 --visits={visit}".format(in_dir=in_dir, rerun=rerun, visit=visit, registries=registries, root_softs=root_softs))


@bash_app(executors=["worker-nodes"], cache=True,  ignore_for_checkpointing=["stdout", "stderr", "wrap"])
def sky_correction(in_dir, rerun, visit, raft_name, inputs=[], stdout=None, stderr=None, wrap=None):
    return wrap("skyCorrection.py {in_dir}  --rerun {rerun} --id visit={visit} raftName={raft_name} --batch-type none --cores 1 --timeout 999999999 --no-versions --loglevel CameraMapper=warn".format(in_dir=in_dir, rerun=rerun, visit=visit, raft_name=raft_name))


##########################################################################
##########################################################################


with open("all_visits_from_register.list") as f:
    visit_lines = f.readlines()

visit_futures = []
for (n, visit_id_unstripped) in zip(range(0, len(visit_lines)), visit_lines):
    visit_id = visit_id_unstripped.strip()

    # some of this stuff could probably be parallelised down tot he per-sensor
    # level rather than per raft. finer granualarity but more overhead in
    # starting up shifter.
    # QUESTION: which bits can go to sensor level?
    # QUESTION: how is efficiency here compared to losing efficiency by runs having wasted long-tail (wall and cpu) time?
    raft_list_fn = "raft_list_for_visit.{}".format(visit_id)

    raft_list_future = raft_list_for_visit(
        configuration.in_dir,
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

    this_visit_single_frame_futs = []

    for (m, raft_name_stripped) in zip(range(0, len(raft_lines)), raft_lines):
        raft_name = raft_name_stripped.strip()
        logger.info("visit {} raft {}".format(visit_id, raft_name))

        # this call is based on run_calexp shell script
        # assume visit_id really is a visit id... workflows/srs/pipe_setups/setup_calexp has a case where the visit file has two fields per line, and this is handled differently there. I have ignored that here.
        # raft_name is the $RAFTNAME environment variable in run_calexp in the XML workflows
        sfd_output_basename = "single_frame_driver.visit-{}.raft-{}".format(n, raft_name)
        this_raft_single_frame_fut = single_frame_driver(
                configuration.in_dir,
                rerun,
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
            configuration.in_dir,
            rerun,
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
    # the workflow looks like you can rnu with a single vist-raft but then the subsequent
    # steps don't take raft as a parameter? so what's the deal there?
    # TODO: assume for now we need to wait for all rafts to be done, and process per visit

    # TODO: which of these post-processing steps need to happen in sequence rather than
    # in parallel?

    check_ccd_stdbase = "check_ccd_astrometry.{}".format(visit_id)
    fut1 = check_ccd_astrometry(
        configuration.root_softs,
        configuration.in_dir,
        rerun,
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
    fut2 = tract2visit_mapper(
        configuration.root_softs,
        configuration.in_dir,
        rerun,
        visit_id,
        inputs=this_visit_single_frame_futs,
        stdout=logdir+tract2visit_mapper_stdbase+".stdout",
        stderr=logdir+tract2visit_mapper_stdbase+".stderr",
        wrap=configuration.wrap)


    visit_futures.append(fut1)
    visit_futures.append(fut2)

    # TODO: visitAnlysis.py for stream and visit - this involves sqlite


logger.info("Waiting for completion of all per-visit tasks")

# wait for them all to complete ...
concurrent.futures.wait(visit_futures)

# ... and throw exception here if any of them threw exceptions
[future.result() for future in visit_futures]

logger.info("Processing tracts")

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

# in order to follow the sequence of job spawning for coaddDriver you need to read in that order :

# setup_fullcoadd, which either look at a provided list of tracts in a file, or build this list out of all the tracts referenced in the tract_visit mapper DB; the it launches one subtask per tract
#    (benc: this workflow should generate the DB from the tract_visit mapper DB)

# setup_patch, which looks in the DB for the list of patches that this tract has (some tracts can have empty patches, especially in DC2), then it subdivides into a small number of patches and launch nested subtasks for each of these subset of patches

# setup_coaddDriver, which takes the tract and the patches provided by setup_patch, lists all the visits that intersect these patches, compare if requested to a provided set of visits (critical to only coadd a given number of years for instance), and then launch one final nested subtask for each filter. This nested subtask runs coaddDriver.py

@bash_app(executors=["worker-nodes"], cache=True,  ignore_for_checkpointing=["stdout", "stderr", "wrap"])
def make_tract_list(in_dir, rerun, stdout=None, stderr=None, wrap=None):
    # this comes from srs/pipe_setups/setup_fullcoadd
    return wrap('sqlite3 {in_dir}/rerun/{rerun}/tracts_mapping.sqlite3 "select DISTINCT tract from overlaps;" > tracts.list'.format(in_dir=in_dir, rerun=rerun))

#    sqlite3 ${OUT_DIR}/rerun/${RERUN1}/tracts_mapping.sqlite3 "select DISTINCT tract from overlaps;" > ${WORKDIR}/all_tracts.list

#    registries = "{in_dir}/rerun/{rerun}/registries".format(in_dir=in_dir, rerun=rerun)

#    return wrap("mkdir -p {registries} && {root_softs}/ImageProcessingPipelines/python/util/tract2visit_mapper.py --indir={in_dir}/rerun/{rerun} --db={registries}/tracts_mapping_{visit}.sqlite3

tract_list_future = make_tract_list(
    configuration.in_dir,
    rerun,
    stdout=logdir+"make_tract_list.stdout",
    stderr=logdir+"make_tract_list.stderr",
    wrap=configuration.wrap)

tract_list_future.result()





logger.info("Reached the end of the parsl driver for DM pipeline")
