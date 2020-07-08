import re

from workflowutils import read_and_strip
from more_itertools import intersperse
from concurrent.futures import Future

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

    patch_id_no_parens = re.sub("[\(\) ]","",patch_id)

    visits = read_and_strip(visit_file)

    if visits == []: # skip is no visits
        # put this definition in future_combiantors TODO
        trivial_future = Future()
        trivial_future.set_result(None)
        return trivial_future

    visit_ids_for_dm = ""
    for el in intersperse("^", visits):
        visit_ids_for_dm += el

    per_visit_futures = []

    for visit in visits:
        input_deps=[]
        if visit_futs:
            input_deps.append(visit_futs[visit])
        per_visit_futures.append(make_coadd_temp_exp(repo_dir, rerun_in, rerun_out, tract_id, patch_id_no_parens, filter_id, visit, inputs=input_deps, obs_lsst_configs=configuration.obs_lsst_configs, wrap=wrap, stdout="{logbase}-visit-{visit}.stdout".format(logbase=logbase, visit=visit), stderr="{logbase}-visit-{visit}.stderr".format(logbase=logbase, visit=visit)))

    fut2 = assemble_coadd(repo_dir, rerun_out, tract_id, patch_id_no_parens, filter_id, visit_ids_for_dm, inputs=per_visit_futures, obs_lsst_configs=configuration.obs_lsst_configs, wrap=wrap, stdout="{logbase}.assemble_coadd.stdout".format(logbase=logbase), stderr="{logbase}.assemble_coadd.stderr".format(logbase=logbase))

    fut3 = detect_coadd_sources(repo_dir, rerun_out, tract_id, patch_id_no_parens, filter_id, visit_file, inputs=[fut2], wrap=wrap, stdout="{logbase}.detect_coadd_sources.stdout".format(logbase=logbase), stderr="{logbase}.detect_coadd_sources.stderr".format(logbase=logbase))

    return fut3

@lsst_app1
def make_coadd_temp_exp(repo_dir, rerun_in, rerun_out, tract_id, patch_id, filter_id, visit_id, obs_lsst_configs, inputs=None, wrap=None):
    f = "makeCoaddTempExp.py {repo_dir}/rerun/{rerun_in} --output {repo_dir}/rerun/{rerun_out} --id tract={tract_id} patch='{patch_id}' filter={filter_id} --selectId visit={visit_id} --configfile {obs_lsst_configs}/makeCoaddTempExp.py --calib {repo_dir}/CALIB".format(repo_dir=repo_dir, rerun_in=rerun_in, rerun_out=rerun_out, tract_id=tract_id, patch_id=patch_id, filter_id=filter_id, visit_id=visit_id, obs_lsst_configs=obs_lsst_configs)
    w = wrap(f)
    return w

@lsst_app1
def assemble_coadd(repo_dir, rerun, tract_id, patch_id, filter_id, visit_ids_for_dm, obs_lsst_configs, inputs=None, wrap=None):
    f = "assembleCoadd.py {repo_dir}/rerun/{rerun} --output {repo_dir}/rerun/{rerun} --id tract={tract_id} patch='{patch_id}' filter={filter_id} --selectId visit={visit_ids_for_dm}  --configfile {obs_lsst_configs}/assembleCoadd.py --calib {repo_dir}/CALIB".format(repo_dir=repo_dir, rerun=rerun, tract_id=tract_id, patch_id=patch_id, filter_id=filter_id, visit_ids_for_dm=visit_ids_for_dm, obs_lsst_configs=obs_lsst_configs)
    w = wrap(f)
    return w

@lsst_app1
def detect_coadd_sources(repo_dir, rerun, tract_id, patch_id, filter_id, visit_ids_for_dm, inputs=None, wrap=None):
    return wrap("detectCoaddSources.py {repo_dir}/rerun/{rerun} --output {repo_dir}/rerun/{rerun} --id tract={tract_id} patch='{patch_id}' filter={filter_id} --calib {repo_dir}/CALIB".format(repo_dir=repo_dir, rerun=rerun, tract_id=tract_id, patch_id=patch_id, filter_id=filter_id))
