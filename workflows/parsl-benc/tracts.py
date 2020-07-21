import re

from workflowutils import read_and_strip
from more_itertools import intersperse
from concurrent.futures import Future
from future_combinators import combine

from parsl import python_app

from lsst_apps import lsst_app1

# coadd_parsl_driver should behave like coadd_driver as much as is reasonable
# but implemented as multiple parsl tasks with increased concurrency. There might
# be further opportunities for concurrency *between* coaddDriver and
# previous/subsequent steps, but for now I am not going to try anything there.

# based on jim's parallelisation diagram:
# https://docs.google.com/presentation/d/1OqluyUvj8LhvVHPEu4IvQoULpHD02QmlB6GCnx9myPY/edit
# slide 7
# makeCoaddTempExp-$FILTER
#   parallelisade over: tract/patch/filter/visit id
# so coaddDriver does not parallelise over visit but the invocations of coadd driver
# are already parallelisad over tract/patch/filter
# then
# assembleCoadd-$FILTER  (which we already parallelise over tract,patch,filter)
# detectCoaddSources-$FILTER (which we alreayd parallelise over tract,patch,filter)
# so re-impl of co-add driver should:

# read visit file
# invoke one makeCoaddTempExp per visit, giving a list of futures
#    what shoudl this depend on? for now, depend on *all* inputs, but actually
#    per-visit stuff only needs to depend on the particular visit future
#    for that visit, not the whole set of visits needed by this coadd step. That
#    will unlock a bunch of concurrency.
#    so do that later.
# invoke one assembleCoadd, giving a future
# invoke one detectCoaddSources, giving a future
# this final future should be the end result future of coadd_parsl_driver


@python_app(executors=['submit-node'])
def coadd_parsl_driver(configuration, rerun_in, rerun_out, tract_id, patch_id, filter_id, visit_file, visit_futs, inputs=None,
                       wrap=None, logbase=""):
    """visit_futs should be:
         a dict of visit id -> visit processing completed future,
      or None if no waiting for visits should happen (for example because
         should be assumed to be completed by some other mechanism.

    This implementation assumes that visit_file is already generated at the
    time of invocation, which is not true in general.
    """

    repo_dir = configuration.repo_dir

    patch_id_no_parens = re.sub("[() ]", "", patch_id)

    visits = read_and_strip(visit_file)

    if visits == []:  # skip if no visits
        # put this definition in future_combinators TODO
        trivial_future = Future()
        trivial_future.set_result(None)
        return trivial_future

    visit_ids_for_dm = ""
    for el in intersperse("^", visits):
        visit_ids_for_dm += el

    per_visit_futures = []

    for visit in visits:
        input_deps = []
        if visit_futs:
            input_deps.append(visit_futs[visit])
        per_visit_futures.append(make_coadd_temp_exp(repo_dir, rerun_in, rerun_out, tract_id, patch_id_no_parens, filter_id, visit, inputs=input_deps, obs_lsst_configs=configuration.obs_lsst_configs, wrap=wrap, stdout="{logbase}-visit-{visit}.stdout".format(logbase=logbase, visit=visit), stderr="{logbase}-visit-{visit}.stderr".format(logbase=logbase, visit=visit), parsl_resource_specification={"priority": 5000}))

    fut2 = assemble_coadd(repo_dir, rerun_out, tract_id, patch_id_no_parens, filter_id, visit_ids_for_dm, inputs=per_visit_futures, obs_lsst_configs=configuration.obs_lsst_configs, wrap=wrap, stdout="{logbase}.assemble_coadd.stdout".format(logbase=logbase), stderr="{logbase}.assemble_coadd.stderr".format(logbase=logbase), parsl_resource_specification={"priority": 5010})

    fut3 = detect_coadd_sources(repo_dir, rerun_out, tract_id, patch_id_no_parens, filter_id, visit_file, inputs=[fut2], wrap=wrap, stdout="{logbase}.detect_coadd_sources.stdout".format(logbase=logbase), stderr="{logbase}.detect_coadd_sources.stderr".format(logbase=logbase), parsl_resource_specification={"priority": 5020})

    return fut3


@lsst_app1
def make_coadd_temp_exp(repo_dir, rerun_in, rerun_out, tract_id, patch_id, filter_id, visit_id, obs_lsst_configs, inputs=None, wrap=None, parsl_resource_specification=None):
    f = "makeCoaddTempExp.py {repo_dir}/rerun/{rerun_in} --output {repo_dir}/rerun/{rerun_out} --id tract={tract_id} patch='{patch_id}' filter={filter_id} --selectId visit={visit_id} --configfile {obs_lsst_configs}/makeCoaddTempExp.py --calib {repo_dir}/CALIB".format(repo_dir=repo_dir, rerun_in=rerun_in, rerun_out=rerun_out, tract_id=tract_id, patch_id=patch_id, filter_id=filter_id, visit_id=visit_id, obs_lsst_configs=obs_lsst_configs)
    w = wrap(f)
    return w


@lsst_app1
def assemble_coadd(repo_dir, rerun, tract_id, patch_id, filter_id, visit_ids_for_dm, obs_lsst_configs, inputs=None, wrap=None, parsl_resource_specification=None):
    f = "assembleCoadd.py {repo_dir}/rerun/{rerun} --output {repo_dir}/rerun/{rerun} --id tract={tract_id} patch='{patch_id}' filter={filter_id} --selectId visit={visit_ids_for_dm}  --configfile {obs_lsst_configs}/assembleCoadd.py --calib {repo_dir}/CALIB".format(repo_dir=repo_dir, rerun=rerun, tract_id=tract_id, patch_id=patch_id, filter_id=filter_id, visit_ids_for_dm=visit_ids_for_dm, obs_lsst_configs=obs_lsst_configs)
    w = wrap(f)
    return w


@lsst_app1
def detect_coadd_sources(repo_dir, rerun, tract_id, patch_id, filter_id, visit_ids_for_dm, inputs=None, wrap=None, parsl_resource_specification=None):
    return wrap("detectCoaddSources.py {repo_dir}/rerun/{rerun} --output {repo_dir}/rerun/{rerun} --id tract={tract_id} patch='{patch_id}' filter={filter_id} --calib {repo_dir}/CALIB".format(repo_dir=repo_dir, rerun=rerun, tract_id=tract_id, patch_id=patch_id, filter_id=filter_id))


@python_app(executors=['submit-node'], join=True)
def multiband_parsl_driver(configuration, rerun_in, rerun_out, tract_id, patch_id_no_parens, filter_list, logbase="", inputs=[], wrap=None):
    """This is a parsl-level replacement for multiBandDriver.py. There is no single final
    task for multiband driver - instead there is one per filter. So the future returned
    by this is a `combine` of those final filter tasks. Something following on from this
    might instead be able to depend only on the final filter tasks."""

    repo_dir = configuration.repo_dir
    # mergeCoaddDetections (one, for all filters on this patch)
    # deblendCoaddSources (per filter)  - depends on single mergeCoaddDetections
    # measureCoaddSources (per filter) - depends on corresponding filter deblend
    # mergeCoaddMeasurements - depends on all filters
    # forcedPhotCoadd (per filter) - depends on mergeCoaddMeasurements
    # 'combine' - future combinator to generate the final status future

    filter_ids_for_dm = ""
    for el in intersperse("^", filter_list):
        filter_ids_for_dm += el

    merge_coadd_det_fut = merge_coadd_detections(repo_dir, rerun_in, rerun_out, tract_id, patch_id_no_parens, obs_lsst_configs=configuration.obs_lsst_configs, filters=filter_ids_for_dm, wrap=wrap, inputs=inputs,  stdout="{logbase}.merge_coadd_detections.stdout".format(logbase=logbase), stderr="{logbase}.merge_coadd_detections.stderr".format(logbase=logbase), parsl_resource_specification={"priority": 6001})

    measure_futs = []
    for filter_id in filter_list:
        deblend_coadd_sources_fut = deblend_coadd_sources(repo_dir, rerun_out, tract_id, patch_id_no_parens, filter_id, wrap=wrap, inputs=[merge_coadd_det_fut], stdout="{logbase}-filter-{filter_id}-deblend_coadd_sources.stdout".format(logbase=logbase, filter_id=filter_id), stderr="{logbase}-filter-{filter_id}-deblend_coadd_sources.stderr".format(logbase=logbase, filter_id=filter_id), parsl_resource_specification={"priority": 6002})

        measure_coadd_sources_fut = measure_coadd_sources(repo_dir, rerun_out, tract_id, patch_id_no_parens, filter_id, obs_lsst_configs=configuration.obs_lsst_configs, wrap=wrap, inputs=[deblend_coadd_sources_fut], stdout="{logbase}-filter-{filter_id}-measure_coadd_sources.stdout".format(logbase=logbase, filter_id=filter_id), stderr="{logbase}-filter-{filter_id}-measure_coadd_sources.stderr".format(logbase=logbase, filter_id=filter_id), parsl_resource_specification={"priority": 6003})

        measure_futs.append(measure_coadd_sources_fut)

    merge_fut = merge_coadd_measurements(repo_dir, rerun_out, tract_id, patch_id_no_parens, obs_lsst_configs=configuration.obs_lsst_configs, wrap=wrap, inputs=measure_futs, stdout="{logbase}-merge-coadd-measurements.stdout".format(logbase=logbase), stderr="{logbase}-merge-coadd-measurements.stderr".format(logbase=logbase), parsl_resource_specification={"priority": 6004})

    forced_phot_coadd_futs = []
    for filter_id in filter_list:
        forced_phot_coadd_future = forced_phot_coadd(repo_dir, rerun_out, tract_id, patch_id_no_parens, filter_id, obs_lsst_configs=configuration.obs_lsst_configs, wrap=wrap, inputs=[merge_fut], stdout="{logbase}-filter-{filter_id}-forced_phot_coadd.stdout".format(filter_id=filter_id, logbase=logbase), stderr="{logbase}-filter-{filter_id}-forced_phot_coadd.stderr".format(filter_id=filter_id, logbase=logbase), parsl_resource_specification=None)
        forced_phot_coadd_futs.append(forced_phot_coadd_future)

    return combine(inputs=forced_phot_coadd_futs)


@lsst_app1
def merge_coadd_detections(repo_dir, rerun_in, rerun_out, tract_id, patch_id, obs_lsst_configs, wrap, filters, inputs=None, parsl_resource_specification=None):
    return wrap("mergeCoaddDetections.py {repo_dir}/rerun/{rerun_in} --output={repo_dir}/rerun/{rerun_out} --id tract={tract_id} patch='{patch_id}' filter={filters}  --configfile {obs_lsst_configs}/mergeCoaddDetections.py".format(repo_dir=repo_dir, rerun_in=rerun_in, rerun_out=rerun_out, tract_id=tract_id, patch_id=patch_id, obs_lsst_configs=obs_lsst_configs, filters=filters))


@lsst_app1
def deblend_coadd_sources(repo_dir, rerun, tract_id, patch_id, filter_id, wrap, inputs=None, parsl_resource_specification=None):
    return wrap("deblendCoaddSources.py {repo_dir} --rerun {rerun} --id tract={tract_id} patch='{patch_id}' filter={filter_id}".format(repo_dir=repo_dir, rerun=rerun, tract_id=tract_id, patch_id=patch_id, filter_id=filter_id))


@lsst_app1
def measure_coadd_sources(repo_dir, rerun, tract_id, patch_id, filter_id, obs_lsst_configs, wrap, inputs=None, parsl_resource_specification=None):
    return wrap("measureCoaddSources.py {repo_dir} --rerun {rerun} --id tract={tract_id} patch='{patch_id}' filter={filter_id} --configfile {obs_lsst_configs}/measureCoaddSources.py".format(repo_dir=repo_dir, rerun=rerun, tract_id=tract_id, patch_id=patch_id, filter_id=filter_id, obs_lsst_configs=obs_lsst_configs))


@lsst_app1
def merge_coadd_measurements(repo_dir, rerun, tract_id, patch_id, obs_lsst_configs, wrap, inputs=None, parsl_resource_specification=None):
    return wrap("mergeCoaddMeasurements.py {repo_dir}/rerun/{rerun} --output {repo_dir}/rerun/{rerun} --id tract={tract_id} patch='{patch_id}' filter=u^g^r^i^z^y  --configfile {obs_lsst_configs}/mergeCoaddMeasurements.py".format(repo_dir=repo_dir, rerun=rerun, tract_id=tract_id, patch_id=patch_id, obs_lsst_configs=obs_lsst_configs))


@lsst_app1
def forced_phot_coadd(repo_dir, rerun, tract_id, patch_id, filter_id, obs_lsst_configs, wrap, inputs=None, stdout=None, stderr=None, parsl_resource_specification=None):
    return wrap("forcedPhotCoadd.py {repo_dir}/rerun/{rerun} --output {repo_dir}/rerun/{rerun} --id tract={tract_id} patch='{patch_id}' filter={filter_id}  --configfile {obs_lsst_configs}/forcedPhotCoadd.py".format(repo_dir=repo_dir, rerun=rerun, tract_id=tract_id, patch_id=patch_id, filter_id=filter_id, obs_lsst_configs=obs_lsst_configs))
