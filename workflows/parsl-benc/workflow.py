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
walltime = "00:27:00"


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

            heartbeat_period = 300,
            heartbeat_threshold = 1201,
            provider=SlurmProvider(
                cori_queue,
                nodes_per_block=compute_nodes,
                exclusive = True,
                init_blocks=1,
                min_blocks=1,
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
truncated_ingest_list = files_to_ingest[0:3]

# we'll then have a list of files that we want to do the "step 1" ingest on
# the implementation of this in SRS is three sets of tasks:
# 1_1 one batches visits, prepares  a load of job scripts and submits them - one per batch of visits
# 1_2 then each of the scripts runs (in parallel)
# 1_3 and then a final one merges the results databases

# Instead:
# I'm going to try one ingest file per parsl task, rather than batching - we'll pay the startup
# cost of the ingest code more, but it will give parsl monitoring and/or checkpointing a better
# view of what is happening

@bash_app(executors=['worker-nodes'], cache=True)
def ingest(file, in_dir, stdout=parsl.AUTO_LOGNAME, stderr=parsl.AUTO_LOGNAME):
    """This comes from workflows/srs/pipe_setups/setup_ingest.
    The NERSC version runs just command; otherwise a bunch of other stuff
    happens - which I'm not implementing here at the moment.

    There SRS workflow using @{chunk_of_ingest_list}, but I'm going to specify a single filename
    directly for now.
    """
    return "ingestDriver.py --batch-type none {in_dir} {arg1} --cores 1 --mode link --output {in_dir} -c clobber=True allowError=True register.ignore=True".format(in_dir=in_dir, arg1=file.strip())

in_dir = "/global/cscratch1/sd/bxc/parslTest/test0"

ingest_futures = [ingest(f, in_dir) for f in truncated_ingest_list]

ingest_results = [future.result() for future in ingest_futures]

logger.info("Reached the end of the parsl driver for DM pipeline")
