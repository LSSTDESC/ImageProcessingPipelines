import dataclasses
import os

from parsl.monitoring import MonitoringHub
from parsl.addresses import address_by_hostname
from parsl.config import Config
from parsl.executors import ThreadPoolExecutor, HighThroughputExecutor
from parsl.launchers import SrunLauncher
from parsl.providers import SlurmProvider
from parsl.utils import get_all_checkpoints
from functools import partial
from typing import Callable

from configuration import WorkflowConfig
from workflowutils import wrap_shifter_container


cori_queue = "debug"
# cori_queue = "priority"

compute_nodes = 1 
walltime = "00:29:30"

# this is the directory that the workflow is invoked in and is where output
# files that don't go in the repo should be put.

workflow_cwd = os.getcwd()

# this is the directory that the workflow .py source code files live in
workflow_src_dir = os.path.dirname(os.path.abspath(__file__))

worker_init = """
cd {workflow_cwd}
source setup.source
export PYTHONPATH={workflow_src_dir}  # to get at workflow modules on remote side
export OMP_NUM_THREADS=1
""".format(workflow_cwd=workflow_cwd, workflow_src_dir=workflow_src_dir)

cori_queue_executor_1 = HighThroughputExecutor(
            label='batch-1',
            address=address_by_hostname(),
            worker_debug=True,

            # this overrides the default HighThroughputExecutor process workers
            # with process workers run inside the appropriate shifter container
            # with lsst setup commands executed. That means that everything
            # running in those workers will inherit the correct environment.
            max_workers=8,
            heartbeat_period=25,
            heartbeat_threshold=75,
            provider=SlurmProvider(
                cori_queue,
                nodes_per_block=compute_nodes,
                exclusive=True,
                init_blocks=0,
                min_blocks=0,
                max_blocks=4,
                scheduler_options="""#SBATCH --constraint=haswell""",
                launcher=SrunLauncher(),
                cmd_timeout=60,
                walltime=walltime,
                worker_init=worker_init,
                parallelism=1.0
            ),
        )

cori_queue_executor_2 = HighThroughputExecutor(
            label='batch-2',
            address=address_by_hostname(),
            worker_debug=True,

            # this overrides the default HighThroughputExecutor process workers
            # with process workers run inside the appropriate shifter container
            # with lsst setup commands executed. That means that everything
            # running in those workers will inherit the correct environment.
            max_workers=8,
            heartbeat_period=25,
            heartbeat_threshold=75,
            provider=SlurmProvider(
                cori_queue,
                nodes_per_block=compute_nodes,
                exclusive=True,
                init_blocks=0,
                min_blocks=0,
                max_blocks=1,
                scheduler_options="""#SBATCH --constraint=haswell""",
                launcher=SrunLauncher(),
                cmd_timeout=60,
                walltime=walltime,
                worker_init=worker_init,
                parallelism=1.0
            ),
        )

local_executor = ThreadPoolExecutor(max_threads=2, label="submit-node")

def wrap_no_op(s):
    return s

cori_shifter_debug_config = WorkflowConfig(
  trim_ingest_list = 600,
  ingest_source="/global/projecta/projectdirs/lsst/production/DC2_ImSim/Run2.1.1i/sim/agn-test",

  rerun_prefix = "benc4",
  visit_min = 0,
  visit_max = 9999999999,
  tract_subset = None,
  patch_subset = None,
  dm_root="/opt/lsst/software/stack",

  obs_lsst_configs="/opt/lsst/software/stack/obs_lsst/config/",


  # this is the butler repo to use
  repo_dir="/global/cscratch1/sd/bxc/lsst-dm-repo-1",

  root_softs="/global/homes/b/bxc/dm/",
  # what is ROOT_SOFTS in general? this has come from the SRS workflow,
  # probably the path to this workflow's repo, up one level.


  # This specifies a function (str -> str) which rewrites a bash command into
  # one appropriately wrapper for whichever container/environment is being used
  # with this configuration (for example, wrap_shifter_container writes the
  # command to a temporary file and then invokes that file inside shifter)
  wrap=partial(wrap_shifter_container, image_id="lsstdesc/desc-drp-stack:v19-dc2-run2.2-v4"),
  wrap_sql=wrap_no_op,

  parsl_config=Config(executors=[cori_queue_executor_1, cori_queue_executor_2, local_executor],
                      strategy='htex',
                      app_cache=True, checkpoint_mode='task_exit',
                      checkpoint_files=get_all_checkpoints(),
                      retries=2,
                      monitoring=MonitoringHub(
                            hub_address=address_by_hostname(),
                            hub_port=55055,
                            monitoring_debug=True,
                            resource_monitoring_interval=10
                      )
                      ))

configuration = cori_shifter_debug_config
