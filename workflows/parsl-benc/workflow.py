#!/usr/bin/env python
import concurrent.futures
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
@bash_app(executors=["worker-nodes"], cache=True, ignore_for_checkpointing=["stdout", "stderr"])
def make_sky_map(wrap, in_dir, rerun, stdout=None, stderr=None):
    return wrap("makeSkyMap.py {} --rerun {}".format(in_dir, rerun))


logger.info("launching makeSkyMap")
rerun = configuration.rerun
skymap_future = make_sky_map(configuration.wrap, configuration.in_dir, rerun, stdout=logdir+"make_sky_map.stdout", stderr=logdir+"make_sky_map.stderr")
skymap_future.result()
logger.info("makeSkyMap completed")

#  setup_calexp: use DB to make a visit file
logger.info("Making visit file from raw_visit table")


@bash_app(executors=["worker-nodes"], cache=True,  ignore_for_checkpointing=["stdout", "stderr"])
def make_visit_file(wrap, in_dir, stdout=None, stderr=None):
    return wrap('sqlite3 {}/registry.sqlite3 "select DISTINCT visit from raw_visit;" > all_visits_from_register.list'.format(in_dir))


visit_file_future = make_visit_file(
    configuration.wrap,
    configuration.in_dir,
    stdout=logdir+"make_visit_file.stdout",
    stderr=logdir+"make_visit_file.stderr")

visit_file_future.result()

logger.info("Finished making visit file")

logger.info("submitting task_calexps")


@bash_app(executors=["worker-nodes"], cache=True,  ignore_for_checkpointing=["stdout", "stderr"])
def single_frame_driver(wrap, in_dir, rerun, visit_id, raft_name, stdout=None, stderr=None):
    # params for stream are WORKDIR=workdir, VISIT=visit_id
    # this is going to be something like found in workflows/srs/pipe_setups/run_calexp
    # run_calexp uses --cores as NSLOTS+1. I'm using cores 1 because I am not sure of
    # the right parallelism here.
#    return wrap("singleFrameDriver.py --batch-type none {in_dir} --rerun {rerun} --id visit={visit} raftName={raft_name} --cores 1 --timeout 999999999 --loglevel CameraMapper=warn".format(in_dir=in_dir, rerun=rerun, visit=visit_id, raft_name=raft_name))


    return wrap("singleFrameDriver.py --batch-type none {in_dir} --rerun {rerun} --id visit={visit} raftName={raft_name} --clobber-versions --cores 1 --timeout 999999999 --loglevel CameraMapper=warn".format(in_dir=in_dir, rerun=rerun, visit=visit_id, raft_name=raft_name))


@bash_app(executors=["worker-nodes"], cache=True,  ignore_for_checkpointing=["stdout", "stderr"])
def raft_list_for_visit(wrap, in_dir, visit_id, out_filename, stderr=None, stdout=None):
    return wrap("sqlite3 {in_dir}/registry.sqlite3 'select distinct raftName from raw where visit={visit_id}' > {out_filename}".format(in_dir=in_dir, visit_id=visit_id, out_filename=out_filename))


# the parsl checkpointing for this won't detect if we ingested more stuff to do with the
# specified visit - I'm not sure quite the right way to do it, and I think its only
# useful in during workflow development when the original ingest list might change?
# would need eg "files in each visit" list to generate a per-visit input "version" id/hash
@bash_app(executors=["worker-nodes"], cache=True,  ignore_for_checkpointing=["stdout", "stderr"])
def check_ccd_astrometry(wrap, root_softs, in_dir, rerun, visit, inputs=[], stderr=None, stdout=None):
    # inputs=[] ignored but used for dependency handling
    return wrap("{root_softs}/ImageProcessingPipelines/python/util/checkCcdAstrometry.py {in_dir}/rerun/{rerun} --id visit={visit} --loglevel CameraMapper=warn".format(visit=visit, rerun=rerun, in_dir=in_dir, root_softs=root_softs))

# the parsl checkpointing for this won't detect if we ingested more stuff to do with the
# specified visit - see comments for check_ccd_astrometry
@bash_app(executors=["worker-nodes"], cache=True,  ignore_for_checkpointing=["stdout", "stderr"])
def tract2visit_mapper(wrap, root_softs, in_dir, rerun, visit, inputs=[], stderr=None, stdout=None):
    # TODO: this seems to be how $REGISTRIES is figured out (via $WORKDIR) perhaps?
    # I'm unsure though
    registries = "{in_dir}/rerun/{rerun}/registries".format(in_dir=in_dir, rerun=rerun)

    return wrap("mkdir -p {registries} && {root_softs}/ImageProcessingPipelines/python/util/tract2visit_mapper.py --indir={in_dir}/rerun/{rerun} --db={registries}/tracts_mapping_{visit}.sqlite3 --visits={visit}".format(in_dir=in_dir, rerun=rerun, visit=visit, registries=registries, root_softs=root_softs))


@bash_app(executors=["worker-nodes"], cache=True,  ignore_for_checkpointing=["stdout", "stderr"])
def sky_correction(wrap, in_dir, rerun, visit, inputs=[], stdout=None, stderr=None):
    return wrap("skyCorrection.py {in_dir}  --rerun {rerun} --id visit={visit} --batch-type none --cores 1 --timeout 999999999 --no-versions --loglevel CameraMapper=warn".format(in_dir=in_dir, rerun=rerun, visit=visit))


##########################################################################
##########################################################################


with open("all_visits_from_register.list") as f:
    visit_lines = f.readlines()

calexp_futs = []
for (n, visit_id_unstripped) in zip(range(0, len(visit_lines)), visit_lines):
    visit_id = visit_id_unstripped.strip()

    raft_list_fn = "raft_list_for_visit.{}".format(visit_id)

    raft_list_future = raft_list_for_visit(
        configuration.wrap,
        configuration.in_dir,
        visit_id,
        raft_list_fn,
        stdout=logdir+raft_list_fn+".stdout",
        stderr=logdir+raft_list_fn+".stderr")
    
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
        sfd_output_basename = "single_frame_driver.visit-{}.raft-{}".format(n, m)
        this_visit_single_frame_futs.append(
            single_frame_driver(
                configuration.wrap,
                configuration.in_dir,
                rerun,
                visit_id,
                raft_name,
                stdout=logdir+sfd_output_basename+".stdout",
                stderr=logdir+sfd_output_basename+".stderr")
        )

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
        configuration.wrap,
        configuration.root_softs,
        configuration.in_dir,
        rerun,
        visit_id,
        inputs=this_visit_single_frame_futs,
        stdout=logdir+check_ccd_stdbase+".stdout",
        stderr=logdir+check_ccd_stdbase+".stderr")


    tract2visit_mapper_stdbase = "tract2visit_mapper.{}".format(visit_id)
    fut2 = tract2visit_mapper(
        configuration.wrap,
        configuration.root_softs,
        configuration.in_dir,
        rerun,
        visit_id,
        inputs=[fut1],
        stdout=logdir+tract2visit_mapper_stdbase+".stdout",
        stderr=logdir+tract2visit_mapper_stdbase+".stderr")

    # this is invoked in run_calexp with $OUT_DIR at the first parameter, but that's not something
    # i've used so far -- so I'm using IN_DIR as used in previous steps
    # TODO: is that the right thing to do? otherwise how does IN_DIR and OUT_DIR differ?
    sky_correction_stdbase = "sky_correction.{}".format(visit_id)
    fut3 = sky_correction(
        configuration.wrap,
        configuration.in_dir,
        rerun,
        visit_id,
        inputs=[fut2],
        stdout=logdir+sky_correction_stdbase+".stdout",
        stderr=logdir+sky_correction_stdbase+".stderr")
    
    calexp_futs.append(fut3)

    # TODO: visitAnlysis.py for stream and visit - this involves sqlite


logger.info("submitted task_calexps. waiting for completion of all of them.")

# wait for them all to complete ...
concurrent.futures.wait(calexp_futs)

# ... and throw exception here if any of them threw exceptions
[future.result() for future in calexp_futs]


# setup_calexp:
#   for each visit line read from visit file, create a task_calexp with that visit as para
#   on LSST-IN2P2 ...
#   ... or on NERSC...
#   split that visit into rafts, and create a task_calexp per (visit,raft)


# finish_calexp - should run after task_calexp.run_calexp


logger.info("Reached the end of the parsl driver for DM pipeline")
