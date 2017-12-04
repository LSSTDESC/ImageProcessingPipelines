import os
import sys
import glob
import pwd
import time
import Pegasus.DAX3 as DAX3
from JobMaker import JobMaker
from repo_tools import *

USER = pwd.getpwuid(os.getuid())[0]

# Create a abstract dag
dax = DAX3.ADAG("DRP_calexp_Pipeline")

# Add some workflow-level metadata
dax.metadata("creator", "%s@%s" % (USER, os.uname()[1]))
dax.metadata("created", time.ctime())

eimage_pattern = os.environ['EIMAGE_PATTERN']
data_repo = os.environ['STACK_DATA_REPO']
config_dir = os.environ['STACK_CONFIG_DIR']
ref_cat_file = os.environ['REF_CAT_FILE']

job_maker = JobMaker(dax, data_repo, config_dir, bin_dir='./bin', tc='tc.txt',
                     default_options=['--doraise', '--clobber-config',
                                      '--clobber-versions'], clobber=True)

# Ingest the raw images.
ingestSimImages = job_maker.make('ingestSimImages', args=(eimage_pattern,),
                                 options={'--output': data_repo},
                                 configfile='ingest.py')

# Ingest the reference catalog.
ingestReferenceCatalog = job_maker.make('ingestReferenceCatalog',
                                        args=(ref_cat_file,),
                                        options={'--output': data_repo},
                                        configfile='DC1_IngestRefConfig.py')

makeDiscreteSkyMap = job_maker.make('makeDiscreteSkyMap')

# Loop over visits
for visit in visit_list(data_repo):
    # Loop over rafts
    for raft in raft_list(visit):
        dataId = dict(visit=visit, raft=raft)
        processCcd = job_maker.make('processEimage', dataId=dataId)
        dax.depends(processCcd, ingestReferenceCatalog)
        dax.depends(makeDiscreteSkyMap, processCcd)

daxfile = 'DRP_calexp.dax'
with open(daxfile, 'w') as f:
    dax.writeXML(f)
