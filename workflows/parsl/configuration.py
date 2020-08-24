# configuration.py

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
from functools import partial
from typing import Callable, List, Optional

from workflowutils import wrap_shifter_container


@dataclasses.dataclass
class WorkflowConfig:
    ingest_source: str
    trim_ingest_list: int

    # only visits numbered inside this range (inclusive) will be processed
    visit_min: int
    visit_max: int

    # only tracts contained in the intersection of the skymap and this list
    # will be processed. Set to None to process all tracts.
    tract_subset: Optional[List[int]]

    # only patches contained in the intersection of the skymap and this list
    # will be processed. Set to None to process all patches.
    patch_subset: Optional[List[int]]

    repo_dir: str
    rerun_prefix: str
    root_softs: str
    dm_root: str
    wrap: Callable[[str], str]
    wrap_sql: Callable[[str], str]
    parsl_config: Config
    obs_lsst_configs: str
    
def load_configuration():
    if len(sys.argv) < 2:
        raise RuntimeError("Specify configuration file as first argument")
    spec = importlib.util.spec_from_file_location('', sys.argv[1])
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.configuration
