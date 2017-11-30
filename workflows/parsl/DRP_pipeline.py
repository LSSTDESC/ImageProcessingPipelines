import os
import sys
import desc.parsl.pipeline_components as pc

eimage_pattern = os.environ['EIMAGE_PATTERN']
ref_cat_file = os.environ['REF_CAT_FILE']

output_repo = pc.set_output_repo('output')
log_files = pc.ParslLogFiles('logs')
log_files.enable = (os.environ['LOG_FILE_ENABLE']=='true')

ref_cat = pc.ingestReferenceCatalog(output_repo, ref_cat_file,
                                    **log_files('ingestReferenceCatalog'))
ref_cat.result()

sim_images = pc.ingestSimImages(output_repo, eimage_pattern,
                                **log_files('ingestSimImages'))
sim_images.result()

jeeves = pc.Jeeves(output_repo)

outputs = []
for visit in jeeves.visits:
    for raft in jeeves.get_rafts(visit):
        dataId = dict(visit=visit, raft=raft)
        outputs.append(pc.processEimage(output_repo, dataId,
                                        **log_files('processEimage_%s' % visit)))
[x.result() for x in outputs]

discrete_sky_map = pc.makeDiscreteSkyMap(output_repo,
                                         **log_files('makeDiscreteSkyMap'))
discrete_sky_map.result()

for patch_id in jeeves.get_patch_ids():
    print("processing patch", patch_id)
    sys.stdout.flush()

    dataId = dict(tract=0, patch=patch_id)

    outputs = jeeves.loop_over_filters(pc.makeCoaddTempExp, 'makeCoaddTempExp',
                                       dataId, log_files, check_patch=False)
    [x.result() for x in outputs]

    outputs = jeeves.loop_over_filters(pc.assembleCoadd, 'assembleCoadd',
                                       dataId, log_files)
    [x.result() for x in outputs]

    coadd_tasks = (
        'detectCoaddSources',
        'mergeCoaddDetections',
        'measureCoaddSources',
        'mergeCoaddMeasurements'
    )

    for task_name in coadd_tasks:
        outputs = jeeves.loop_over_filters(run_coadd_task, task_name,
                                           dataId, log_files)
        [x.result() for x in outputs]
