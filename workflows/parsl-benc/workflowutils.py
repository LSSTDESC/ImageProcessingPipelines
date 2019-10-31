
def wrap_shifter_container(cmd: str) -> str:
    """given a command, creates a new command that runs the original
    command inside an LSST application container. There is a lot of
    dancing around with cwd, because cwd is not preserved across
    container invocation (at least, it wasn't in singularity which needs
    supporting eventually)
    """
    import os
    import platform
    import time
    cmdfile = "./wrap-container.{}.{}.{}".format(platform.node(), os.getpid(), time.time())
    with open(cmdfile, "w") as f:
        f.write(cmd)
    
    return "shifter --image=lsstdesc/stack:w_2019_19-dc2_run2.1i {cwd}/container-inner.sh {cwd} {cmd}".format(cmd=cmdfile, cwd=os.getcwd())

