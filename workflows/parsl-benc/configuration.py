import dataclasses
import importlib
import os
import sys

from parsl.monitoring import MonitoringHub
from parsl.addresses import address_by_hostname
from parsl.config import Config
from parsl.executors import ThreadPoolExecutor, HighThroughputExecutor
from parsl.launchers import SrunLauncher
from parsl.providers import SlurmProvider
from parsl.utils import get_all_checkpoints
from typing import Callable

from workflowutils import wrap_shifter_container


@dataclasses.dataclass
class WorkflowConfig:
    ingest_source: str
    in_dir: str
    rerun: str
    root_softs: str
    wrap: Callable[[str], str]
    parsl_config: Config

def load_configuration():
    if len(sys.argv) < 2:
        raise RuntimeError("Specify configuration file as first argument")
    spec = importlib.util.spec_from_file_location('', sys.argv[1])
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.configuration

cori_queue = "debug"

# aside from maxwalltime/discount/queue limit considerations, it is probably
# better to increase max_blocks rather than compute_nodes to fit into schedule
# more easily?
max_blocks = 3

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
""".format(workflow_cwd=workflow_cwd, workflow_src_dir=workflow_src_dir)

cori_queue_executor = HighThroughputExecutor(
            label='worker-nodes',
            address=address_by_hostname(),
            worker_debug=True,

            # this overrides the default HighThroughputExecutor process workers
            # with process workers run inside the appropriate shifter container
            # with lsst setup commands executed. That means that everything
            # running in those workers will inherit the correct environment.

            heartbeat_period=25,
            heartbeat_threshold=75,
            provider=SlurmProvider(
                cori_queue,
                nodes_per_block=compute_nodes,
                exclusive=True,
                init_blocks=0,
                min_blocks=0,
                max_blocks=max_blocks,
                scheduler_options="""#SBATCH --constraint=haswell""",
                launcher=SrunLauncher(),
                cmd_timeout=60,
                walltime=walltime,
                worker_init=worker_init
            ),
        )

local_executor = ThreadPoolExecutor(max_threads=2, label="submit-node")

cori_shifter_debug_config = WorkflowConfig(
  ingest_source="/global/projecta/projectdirs/lsst/production/DC2_ImSim/Run2.1.1i/sim/agn-test",

  # this is the butler repo to use
  in_dir="/global/cscratch1/sd/bxc/parslTest/test0",
  rerun="some_rerun",

  root_softs="/global/homes/b/bxc/dm/",
  # what is ROOT_SOFTS in general? this has come from the SRS workflow,
  # probably the path to this workflow's repo, up one level.


  # This specifies a function (str -> str) which rewrites a bash command into
  # one appropriately wrapper for whichever container/environment is being used
  # with this configuration (for example, wrap_shifter_container writes the
  # command to a temporary file and then invokes that file inside shifter)
  wrap=wrap_shifter_container,

  parsl_config=Config(executors=[local_executor, cori_queue_executor],
                      app_cache=True, checkpoint_mode='task_exit',
                      checkpoint_files=get_all_checkpoints(),
                      monitoring=MonitoringHub(
                            hub_address=address_by_hostname(),
                            hub_port=55055,
                            monitoring_debug=False,
                            resource_monitoring_interval=10
                      )))

configuration = cori_shifter_debug_config
