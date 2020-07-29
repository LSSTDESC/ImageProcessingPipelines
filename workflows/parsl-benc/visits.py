from lsst_apps import lsst_app1

@lsst_app1
def single_frame_driver(repo_dir, rerun, visit_id, raft_name,
                        stdout=None, stderr=None, wrap=None,
                        parsl_resource_specification=None):
    # params for stream are WORKDIR=workdir, VISIT=visit_id
    # this is going to be something like found in
    # workflows/srs/pipe_setups/run_calexp
    # run_calexp uses --cores as NSLOTS+1. I'm using cores 1 because I
    # am not sure of the right parallelism here.

    return wrap(("processCcd.py {repo_dir} "
                 "--rerun {rerun} "
                 "--id visit={visit} raftName={raft_name} "
                 "--calib {repo_dir}/CALIB/ "
                 "--clobber-versions").format(repo_dir=repo_dir,
                                              rerun=rerun,
                                              visit=visit_id,
                                              raft_name=raft_name))

@lsst_app1
def sky_correction(repo_dir, rerun, visit, raft_name, inputs=[], stdout=None, stderr=None, wrap=None, parsl_resource_specification=None):
    return wrap("skyCorrection.py {repo_dir}  --rerun {rerun} --id visit={visit} raftName={raft_name} --batch-type none --cores 1  --calib {repo_dir}/CALIB/ --timeout 999999999 --no-versions --loglevel CameraMapper=warn".format(repo_dir=repo_dir, rerun=rerun, visit=visit, raft_name=raft_name))


@lsst_app1
def check_ccd_astrometry(dm_root, repo_dir, rerun, visit, inputs=[],
                         stderr=None, stdout=None, wrap=None, parsl_resource_specification=None):
    # inputs=[] ignored but used for dependency handling
    return wrap("{dm_root}/ImageProcessingPipelines/python/util/checkCcdAstrometry.py {repo_dir}/rerun/{rerun} "
                "--id visit={visit} "
                "--loglevel CameraMapper=warn".format(visit=visit,
                                                      rerun=rerun,
                                                      repo_dir=repo_dir,
                                                      dm_root=dm_root))
