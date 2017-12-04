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
dax = DAX3.ADAG("DRP_Pipeline")

# Add some workflow-level metadata
dax.metadata("creator", "%s@%s" % (USER, os.uname()[1]))
dax.metadata("created", time.ctime())

eimage_pattern = os.environ['EIMAGE_PATTERN']
data_repo =  os.environ['STACK_DATA_REPO']
config_dir = os.environ['STACK_CONFIG_DIR']
ref_cat_file = os.environ['REF_CAT_FILE']

default_options = ['--doraise',
                   '--clobber-config',
                   '--clobber-versions',]
job_maker = JobMaker(dax, data_repo, config_dir, bin_dir='./bin', tc='tc.txt',
                     default_options=default_options, clobber=True)

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
        processCcd = job_maker.make('processEimage', dataId=dataId,
                                    options={'--output': data_repo})
        dax.depends(processCcd, ingestReferenceCatalog)
        dax.depends(makeDiscreteSkyMap, processCcd)

# Loop over tracts
for tract in tract_list(data_repo):
    # Loop over patches.
    for patch in patch_list(data_repo, tract=tract):
        dataId = dict(patch=patch, tract=tract, filter='^'.join(filter_list()))
        mergeDetections = job_maker.make('mergeDetections', dataId=dataId)
        for filt in filter_list():
            dataId = dict(patch=patch, tract=tract, filter=filt)
            options = {'--selectId': 'filter=%s' % filt}
            makeTempExpCoadd = job_maker.make('makeTempExpCoadd', dataId=dataId,
                                              options=options)
            dax.depends(makeTempExpCoadd, makeDiscreteSkyMap)

            assembleCoadd = job_maker.make('assembleCoadd', dataId=dataId,
                                           options=options)
            dax.depends(assembleCoadd, makeTempExpCoadd)

            detectCoaddSources = job_maker.make('detectCoaddSources',
                                                dataId=dataId)
            dax.depends(detectCoaddSources, assembleCoadd)
            dax.depends(mergeDetections, detectCoaddSources)

        # Make a separate loop over filters for measureCoadd job
        # since it will take place after mergeDetections has run on
        # all filters.
        for filt in filter_list():
            dataId = dict(patch=patch, tract=tract, filter=filt)
            measureCoadd = job_maker.make('measureCoadd', dataId=dataId)
            dax.depends(measureCoadd, mergeDetections)

# Forced photometry on data for each visit.
for visit in visit_list(data_repo):
    for raft in raft_list(visit):
        for sensor in sensor_list(visit, raft):
            dataId = dict(visit=visit, raft=raft, sensor=sensor)
            forcedPhotCcd = job_maker.make('forcedPhotCcd', dataId=dataId)

daxfile = 'DRP.dax'
with open(daxfile, 'w') as f:
    dax.writeXML(f)
