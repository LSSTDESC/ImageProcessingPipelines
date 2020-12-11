from lsst_apps import lsst_app1


@lsst_app1
def single_frame_driver(repo_dir, inrepo, outrepo, visit_id, raft_name,
                        stdout=None, stderr=None, wrap=None,
                        parsl_resource_specification=None):
    # params for stream are WORKDIR=workdir, VISIT=visit_id
    # this is going to be something like found in
    # workflows/srs/pipe_setups/run_calexp
    # run_calexp uses --cores as NSLOTS+1. I'm using cores 1 because I
    # am not sure of the right parallelism here.

    return wrap((f'processCcd.py {inrepo} '
                 f'--output {outrepo} '
                 f'--id visit={visit_id} raftName={raft_name} '
                 f'--calib {repo_dir}/CALIB/ '
                 f'--clobber-versions'))


@lsst_app1
def sky_correction(repo_dir, inrepo, outrepo, visit, raft_name, inputs=[], stdout=None, stderr=None, wrap=None, parsl_resource_specification=None):
    return wrap((f"skyCorrection.py {inrepo} "
                 f"--output {outrepo} "
                 f"--id visit={visit} raftName={raft_name} "
                 f"--batch-type none "
                 f"--cores 1  "
                 f"--calib {repo_dir}/CALIB/ "
                 f"--timeout 999999999 "
                 f"--no-versions "
                 f"--loglevel CameraMapper=warn "))


@lsst_app1
def check_ccd_astrometry(dm_root, repo_dir, rerun, visit_id, inputs=[],
                         stderr=None, stdout=None, wrap=None, parsl_resource_specification=None):
    # inputs=[] ignored but used for dependency handling
    return wrap((f"{dm_root}/ImageProcessingPipelines/python/util/checkCcdAstrometry.py {repo_dir}/rerun/{rerun} "
                f"--id visit={visit_id} "
                f"--loglevel CameraMapper=warn "))
