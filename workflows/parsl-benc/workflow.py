import concurrent.futures
import logging

import parsl
from parsl import bash_app
from parsl.monitoring import MonitoringHub
from parsl.addresses import address_by_hostname
from parsl.config import Config
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
def create_ingest_file_list(pipe_scripts_dir, ingest_source):
    return "{pipe_scripts_dir}/createIngestFileList.py {ingest_source} --recursive --ext .fits".format(pipe_scripts_dir=pipe_scripts_dir, ingest_source=ingest_source)

pipe_scripts_dir = "/global/homes/b/bxc/dm/ImageProcessingPipelines/workflows/srs/pipe_scripts/"
ingest_source = "/global/projecta/projectdirs/lsst/production/DC2_ImSim/Run2.1.1i/sim/agn-test"
ingest_list_future = create_ingest_file_list(pipe_scripts_dir, ingest_source)

# this gives about 45000 files listed in "filesToIngest.txt"



# heather then advises cutting out two specific files - although I only see the second at the
# moment so I'm only filtering that out...

# (see  https://github.com/LSSTDESC/DC2-production/issues/359#issuecomment-521330263 )
# Two centroid files were identified as failing the centroid check and will be omitted from processing: 00458564 (R32 S21) and 00466748 (R43 S21)

@bash_app(executors=["submit-node"], cache=True)
def filter_in_place(ingest_list_future):
    return "grep --invert-match 466748_R43_S21 filesToIngest.txt > filter-filesToIngest.tmp && mv filter-filesToIngest.tmp filesToIngest.txt"

filtered_ingest_list_future = filter_in_place(ingest_list_future)

filtered_ingest_list_future.result()

with open("filesToIngest.txt") as f:
    files_to_ingest = f.readlines()

logger.info("There are {} entries in ingest list".format(len(files_to_ingest)))

# for testing, truncated this list heavilty
truncated_ingest_list = files_to_ingest[0:50]

logger.info("writing truncated list")
truncatedFileList= "filesToIngestTruncated.txt"
with open(truncatedFileList, "w") as f:
    f.writelines(truncated_ingest_list) # caution line endings - writelines needs them, apparently but unsure if readlines trims them off
logger.info("wrote truncated list")

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
    return "ingestDriver.py --batch-type none {in_dir} @{arg1} --cores 1 --mode link --output {in_dir} -c clobber=True allowError=True register.ignore=True".format(in_dir=in_dir, arg1=file.strip())

in_dir = "/global/cscratch1/sd/bxc/parslTest/test0"

#ingest_futures = [run_ingest(f, in_dir, n) for (f, n) in zip(truncated_ingest_list, range(0,len(truncated_ingest_list)))]
ingest_futures = [run_ingest(truncatedFileList, in_dir, 0)] 

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

@bash_app(executors=["worker-nodes"], cache=True)
def make_sky_map(in_dir, rerun, stdout=None, stderr=None):
    return "makeSkyMap.py {} --rerun {}".format(in_dir, rerun)

logger.info("launching makeSkyMap")
rerun = "some_rerun"
skymap_future = make_sky_map(in_dir, rerun, stdout="make_sky_map.stdout", stderr="make_sky_map.stderr")
skymap_future.result()
logger.info("makeSkyMap completed")

#  setup_calexp: use DB to make a visit file
logger.info("Making visit file from raw_visit table")

@bash_app(executors=["worker-nodes"], cache=True)
def make_visit_file(in_dir):
    return 'sqlite3 {}/registry.sqlite3 "select DISTINCT visit from raw_visit;" > all_visits_from_register.list'.format(in_dir)

visit_file_future = make_visit_file(in_dir)
visit_file_future.result()

logger.info("Finished making visit file")

logger.info("submitting task_calexps")


@bash_app(executors=["worker-nodes"], cache=True)
def task_calexp(in_dir, rerun, visit_id, stdout=None, stderr=None):
    # params for stream are WORKDIR=workdir, VISIT=visit_id
    # this is going to be something like found in workflows/srs/pipe_setups/run_calexp
    # run_calexp uses --cores as NSLOTS+1. I'm using cores 1 because I am not sure of
    # the right parallelism here.
    return "singleFrameDriver.py --batch-type none {in_dir} --rerun {rerun} --id visit={visit} --cores 1 --timeout 999999999 --loglevel CameraMapper=warn".format(in_dir=in_dir, rerun=rerun, visit=visit_id)

with open("all_visits_from_register.list") as f:
    visit_lines = f.readlines()

calexp_futs = []
for (n, visit_id_unstripped) in zip(range(0,len(visit_lines)), visit_lines):
    visit_id = visit_id_unstripped.strip()
    # assume visit_id really is a visit id... workflows/srs/pipe_setups/setup_calexp has a case where the visit file has two fields per line, and this is handled differently there. I have ignored that here.
    calexp_futs.append(task_calexp(in_dir, rerun, visit_id, stdout="task_calexp.{}.stdout".format(n), stderr="task_calexp.{}.stderr".format(n)))

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
