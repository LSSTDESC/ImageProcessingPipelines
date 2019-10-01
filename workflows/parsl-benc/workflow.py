import concurrent.futures
import logging
import os

import parsl
from parsl import bash_app, python_app
from parsl.monitoring import MonitoringHub
from parsl.addresses import address_by_hostname
from parsl.config import Config
from parsl.data_provider.files import File # TODO: in parsl, export File from parsl.data_provider top
from parsl.dataflow.memoization import id_for_memo
from parsl.executors import ThreadPoolExecutor, HighThroughputExecutor
from parsl.launchers import SrunLauncher
from parsl.providers import SlurmProvider
from parsl.utils import get_all_checkpoints



# assumption: this is running with the same-ish python as inside the expected
# container:

# eg. on the outside:
#   module load python/3.7-anaconda-2019.07
#   source activate dm-play
# with parsl installed in that container


# OLD:
# at least this much, inside a suitable container:
#  source /opt/lsst/software/stack/loadLSST.bash

#  setup lsst_distrib
#  setup obs_lsst

logger = logging.getLogger("parsl.dm")

parsl.set_stream_logger()

logger.info("Parsl driver for DM pipeline")

local_executor = ThreadPoolExecutor(max_threads=2, label="submit-node")

cori_queue = "debug"
max_blocks = 3 # aside from maxwalltime/discount/queue limit considerations, it is probably
               # better to increase max_blocks rather than compute_nodes to fit into schedule
               # more easily?
compute_nodes = 1
walltime = "00:29:30"

@id_for_memo.register(File)
def id_for_memo_File(f, output_ref=False):
    if output_ref:
        logger.debug("hashing File as output ref without content: {}".format(f))
        return f.url
    else:
        logger.debug("hashing File as input with content: {}".format(f))
        assert f.scheme == "file"
        filename = f.filepath
        stat_result = os.stat(filename)

        return [f.url, stat_result.st_size, stat_result.st_mtime]


class CoriShifterSRunLauncher:
    def __init__(self):
        self.srun_launcher = SrunLauncher()

    def __call__(self, command, tasks_per_node, nodes_per_block):
        new_command="/global/homes/b/bxc/dm/ImageProcessingPipelines/workflows/parsl-benc/worker-wrapper {}".format(command)
        return self.srun_launcher(new_command, tasks_per_node, nodes_per_block)

cori_queue_executor = HighThroughputExecutor(
            label='worker-nodes',
            address=address_by_hostname(),
            worker_debug=True,

            # this overrides the default HighThroughputExecutor process workers with
            # process workers run inside the appropriate shifter container with
            # lsst setup commands executed. That means that everything running in
            # those workers will inherit the correct environment.

            heartbeat_period = 25,
            heartbeat_threshold = 75,
            provider=SlurmProvider(
                cori_queue,
                nodes_per_block=compute_nodes,
                exclusive = True,
                init_blocks=0,
                min_blocks=0,
                max_blocks=max_blocks,
                scheduler_options="""#SBATCH --constraint=haswell""",
                launcher=CoriShifterSRunLauncher(),
                cmd_timeout=60,
                walltime=walltime
            ),
        )

config = Config(executors=[local_executor, cori_queue_executor],
                app_cache=True, checkpoint_mode='task_exit',
                checkpoint_files=get_all_checkpoints(),
                monitoring=MonitoringHub(
                    hub_address=address_by_hostname(),
                    hub_port=55055,
                    logging_level=logging.INFO,
                    resource_monitoring_interval=10
                ))

parsl.load(config)


@bash_app(executors=["worker-nodes"], cache=True)
def create_ingest_file_list(pipe_scripts_dir, ingest_source, outputs=[]):
    outfile = outputs[0]
    return "{pipe_scripts_dir}/createIngestFileList.py {ingest_source} --recursive --ext .fits && mv filesToIngest.txt {out_fn}".format(pipe_scripts_dir=pipe_scripts_dir, ingest_source=ingest_source, out_fn=outfile.filepath)

pipe_scripts_dir = "/global/homes/b/bxc/dm/ImageProcessingPipelines/workflows/srs/pipe_scripts/"
ingest_source = "/global/projecta/projectdirs/lsst/production/DC2_ImSim/Run2.1.1i/sim/agn-test"
ingest_file = File("wf_files_to_ingest")


ingest_fut = create_ingest_file_list(pipe_scripts_dir, ingest_source, outputs=[ingest_file])
# this gives about 45000 files listed in ingest_file

ingest_file_output_file = ingest_fut.outputs[0] # same as ingest_file but with dataflow ordering

# heather then advises cutting out two specific files - although I only see the second at the
# moment so I'm only filtering that out...

# (see  https://github.com/LSSTDESC/DC2-production/issues/359#issuecomment-521330263 )
# Two centroid files were identified as failing the centroid check and will be omitted from processing: 00458564 (R32 S21) and 00466748 (R43 S21)

@bash_app(executors=["submit-node"], cache=True)
def filter_in_place(ingest_file):
    return "grep --invert-match 466748_R43_S21 {} > filter-filesToIngest.tmp && mv filter-filesToIngest.tmp filesToIngest.txt".format(ingest_file.filepath)

filtered_ingest_list_future = filter_in_place(ingest_file_output_file)

filtered_ingest_list_future.result()

with open("filesToIngest.txt") as f:
    files_to_ingest = f.readlines()

logger.info("There are {} entries in ingest list".format(len(files_to_ingest)))

# for testing, truncated this list heavilty
@python_app(executors=["submit-node"], cache=True)
def truncate_ingest_list(files_to_ingest, n, outputs=[]):
    l = files_to_ingest[0:n]
    logger.info("writing truncated list")
    with open(outputs[0].filepath, "w") as f:
        f.writelines(l) # caution line endings - writelines needs them, apparently but unsure if readlines trims them off
    logger.info("wrote truncated list")

truncatedFileListName= "wf_FilesToIngestTruncated.txt"
truncatedFileList = File(truncatedFileListName)
truncated_ingest_list = truncate_ingest_list(files_to_ingest, 20, outputs=[truncatedFileList])
truncatedFileList_output_future = truncated_ingest_list.outputs[0] # future form of truncatedFileList
# parsl discussion: the UI is awkward that we can make a truncatedFileList
# File but then we need to extract out the datafuture that contains "the same"
# file to get dependency ordering.


# we'll then have a list of files that we want to do the "step 1" ingest on
# the implementation of this in SRS is three sets of tasks:
# 1_1 one batches visits, prepares  a load of job scripts and submits them - one per batch of visits
# 1_2 then each of the scripts runs (in parallel)
# 1_3 and then a final one merges the results databases

# Instead:
# I'm going to try one ingest file per parsl task, rather than batching - we'll pay the startup
# cost of the ingest code more, but it will give parsl monitoring and/or checkpointing a better
# view of what is happening

def run_ingest(file, in_dir, n):
    return ingest(file, in_dir, stdout="ingest.{}.stdout".format(n), stderr="ingest.{}.stderr".format(n))

@bash_app(executors=['worker-nodes'], cache=True)
def ingest(file, in_dir, stdout=None, stderr=None):
    # parsl.AUTO_LOGNAME does not work with checkpointing: see https://github.com/Parsl/parsl/issues/1293
    # def ingest(file, in_dir, stdout=parsl.AUTO_LOGNAME, stderr=parsl.AUTO_LOGNAME):
    """This comes from workflows/srs/pipe_setups/setup_ingest.
    The NERSC version runs just command; otherwise a bunch of other stuff
    happens - which I'm not implementing here at the moment.

    There SRS workflow using @{chunk_of_ingest_list}, but I'm going to specify a single filename
    directly for now.
    """
    return "ingestDriver.py --batch-type none {in_dir} @{arg1} --cores 1 --mode link --output {in_dir} -c clobber=True allowError=True register.ignore=True".format(in_dir=in_dir, arg1=file.filepath)

in_dir = "/global/cscratch1/sd/bxc/parslTest/test0"

#ingest_futures = [run_ingest(f, in_dir, n) for (f, n) in zip(truncated_ingest_list, range(0,len(truncated_ingest_list)))]
ingest_futures = [run_ingest(truncatedFileList_output_future, in_dir, 0)] 

# this will wait for all futures to complete before proceeding
# and then any exceptions will be thrown in ingest_results
# comprehension afterwards. This gives opportunity for everything
# to run before hitting an exception.
logger.info("waiting for ingest(s) to complete")
[future.exception() for future in ingest_futures]

ingest_results = [future.result() for future in ingest_futures]

logger.info("ingest(s) completed")

# now equivalent of DC2DM_2_SINGLEFRAME_NERSC.xml

# setup_calexp .... eg workflows/srs/pipe_setups/setup_calexp

#   makeSkyMap.py
#   QUESTION: in xml, this does copying of files out of one rerun dir into another, neither of which is the rerun dir passed to makeSkyMap... what is going on there? I'm going to ignore reruns entirely here if i can...

# QUESTION: what is the concurrency between make_sky_map and the raw visit list? can they run concurrently or must make_sky_map run before generating the raw visit list?

# ingest list is passed in but not used explicity because it represents that some stuff
# has gone into the DB potentially during ingest - for checkpointing
@bash_app(executors=["worker-nodes"], cache=True)
def make_sky_map(in_dir, rerun, ingest_list, stdout=None, stderr=None):
    return "makeSkyMap.py {} --rerun {}".format(in_dir, rerun)

logger.info("launching makeSkyMap")
rerun = "some_rerun"
skymap_future = make_sky_map(in_dir, rerun, truncatedFileList_output_future, stdout="make_sky_map.stdout", stderr="make_sky_map.stderr")
skymap_future.result()
logger.info("makeSkyMap completed")

#  setup_calexp: use DB to make a visit file
logger.info("Making visit file from raw_visit table")

@bash_app(executors=["worker-nodes"], cache=True)
def make_visit_file(in_dir, ingest_list):
    return 'sqlite3 {}/registry.sqlite3 "select DISTINCT visit from raw_visit;" > all_visits_from_register.list'.format(in_dir)

visit_file_future = make_visit_file(in_dir, truncatedFileList_output_future)
visit_file_future.result()

logger.info("Finished making visit file")

logger.info("submitting task_calexps")


@bash_app(executors=["worker-nodes"], cache=True)
def single_frame_driver(in_dir, rerun, visit_id, raft_name, stdout=None, stderr=None):
    # params for stream are WORKDIR=workdir, VISIT=visit_id
    # this is going to be something like found in workflows/srs/pipe_setups/run_calexp
    # run_calexp uses --cores as NSLOTS+1. I'm using cores 1 because I am not sure of
    # the right parallelism here.
    return "singleFrameDriver.py --batch-type none {in_dir} --rerun {rerun} --id visit={visit} raftName={raft_name} --cores 1 --timeout 999999999 --loglevel CameraMapper=warn".format(in_dir=in_dir, rerun=rerun, visit=visit_id, raft_name=raft_name)


@bash_app(executors=["worker-nodes"], cache=True)
def raft_list_for_visit(in_dir, visit_id, out_filename):
    return "sqlite3 {in_dir}/registry.sqlite3 'select distinct raftName from raw where visit={visit_id}' > {out_filename}".format(in_dir = in_dir, visit_id = visit_id, out_filename = out_filename)


# the parsl checkpointing for this won't detect if we ingested more stuff to do with the
# specified visit - I'm not sure quite the right way to do it, and I think its only
# useful in during workflow development when the original ingest list might change?
# would need eg "files in each visit" list to generate a per-visit input "version" id/hash
@bash_app(executors=["worker-nodes"], cache=True)
def check_ccd_astrometry(in_dir, rerun, visit, inputs=[]):
    # inputs=[] ignored but used for dependency handling
    # TODO: what is ROOT_SOFTS? probably the path to this workflow's repo.
    root_softs="/global/homes/b/bxc/dm/"
    return "{root_softs}/ImageProcessingPipelines/python/util/checkCcdAstrometry.py {in_dir}/rerun/{rerun} --id visit={visit} --loglevel CameraMapper=warn".format(visit=visit, rerun=rerun, in_dir=in_dir, root_softs=root_softs)

# the parsl checkpointing for this won't detect if we ingested more stuff to do with the
# specified visit - see comments for check_ccd_astrometry
@bash_app(executors=["worker-nodes"], cache=True)
def tract2visit_mapper(in_dir, rerun, visit, inputs=[], stderr=None, stdout=None):
    root_softs="/global/homes/b/bxc/dm/"

    # TODO: this seems to be how $REGISTRIES is figured out (via $WORKDIR) perhaps?
    # I'm unsure though
    registries="{in_dir}/rerun/{rerun}/registries".format(in_dir=in_dir, rerun=rerun)

    return "mkdir -p {registries} && {root_softs}/ImageProcessingPipelines/python/util/tract2visit_mapper.py --indir={in_dir}/rerun/{rerun} --db={registries}/tracts_mapping_{visit}.sqlite3 --visits={visit}".format(in_dir=in_dir, rerun=rerun, visit=visit, registries=registries, root_softs=root_softs)


@bash_app(executors=["worker-nodes"], cache=True)
def sky_correction(in_dir, rerun, visit, inputs=[], stdout=None, stderr=None):
    return "skyCorrection.py {in_dir}  --rerun {rerun} --id visit={visit} --batch-type none --cores 1 --timeout 999999999 --no-versions --loglevel CameraMapper=warn".format(in_dir=in_dir, rerun=rerun, visit=visit)

with open("all_visits_from_register.list") as f:
    visit_lines = f.readlines()

calexp_futs = []
for (n, visit_id_unstripped) in zip(range(0,len(visit_lines)), visit_lines):
    visit_id = visit_id_unstripped.strip()
  
    raft_list_fn = "raft_list_for_visit.{}".format(visit_id)

    raft_list_future = raft_list_for_visit(in_dir, visit_id, raft_list_fn)
    raft_list_future.result()
    # this wait here means that we don't get parallelisation so much
    # there are problems with launching tasks within tasks due to locking up
    # a local worker... so avoid doing that.
    # i.e. the monadness

    with open(raft_list_fn) as f:
        raft_lines = f.readlines()

    this_visit_single_frame_futs = []

    for (m, raft_name_stripped) in zip(range(0,len(raft_lines)), raft_lines):
        raft_name=raft_name_stripped.strip()
        logger.info("visit {} raft {}".format(visit_id, raft_name))

        # this call is based on run_calexp shell scriprt
        # assume visit_id really is a visit id... workflows/srs/pipe_setups/setup_calexp has a case where the visit file has two fields per line, and this is handled differently there. I have ignored that here.
        # raft_name is the $RAFTNAME environment variable in run_calexp in the XML workflows
        sfd_output_basename="single_frame_driver.{}.{}".format(m,n)
        this_visit_single_frame_futs.append(single_frame_driver(in_dir, rerun, visit_id, raft_name, stdout=sfd_output_basename+".stdout", stderr=sfd_output_basename+".stderr"))

    # now need to join based on all of this_visit_single_frame_futs... but not in sequential code
    # because otherwise we won't launch later visits until after we're done with this one, and
    # lose parallelism
    # question here: should these be done per-raft or per-visit?
    # the workflow looks like you can rnu with a single vist-raft but then the subsequent
    # steps don't take raft as a parameter? so what's the deal there?
    # TODO: assume for now we need to wait for all rafts to be done, and process per visit

    # TODO: which of these post-processing steps need to happen in sequence rather than
    # in parallel?

    fut1 = check_ccd_astrometry(in_dir, rerun, visit_id, inputs=this_visit_single_frame_futs)

    tract2visit_mapper_stdbase = "track2visit_mapper.{}".format(visit_id)
    fut2 = tract2visit_mapper(in_dir, rerun, visit_id, inputs=[fut1], stdout=tract2visit_mapper_stdbase+".stdout", stderr=tract2visit_mapper_stdbase+".stderr")


    # this is invoked in run_calexp with $OUT_DIR at the first parameter, but that's not something
    # i've used so far -- so I'm using IN_DIR as used in previous steps
    # TODO: is that the right thing to do? otherwise how does IN_DIR and OUT_DIR differ?
    sky_correction_stdbase = "sky_correction.{}".format(visit_id)
    fut3 = sky_correction(in_dir, rerun, visit_id, inputs=[fut2], stdout=sky_correction_stdbase+".stdout", stderr=sky_correction_stdbase+".stderr")

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
