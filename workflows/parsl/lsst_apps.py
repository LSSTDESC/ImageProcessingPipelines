from parsl import bash_app

lsst_app1 = bash_app(executors=["batch-1"],
                     cache=True,
                     ignore_for_cache=["stdout", "stderr", "wrap", "parsl_resource_specification"])

lsst_app2 = bash_app(executors=["batch-2"],
                     cache=True,
                     ignore_for_cache=["stdout", "stderr", "wrap", "parsl_resource_specification"])

lsst_app3 = bash_app(executors=["batch-3"],
                     cache=True,
                     ignore_for_cache=["stdout", "stderr", "wrap", "parsl_resource_specification"])

lsst_app4 = bash_app(executors=["batch-4"],
                     cache=True,
                     ignore_for_cache=["stdout", "stderr", "wrap", "parsl_resource_specification"])

lsst_app5 = bash_app(executors=["batch-5"],
                     cache=True,
                     ignore_for_cache=["stdout", "stderr", "wrap", "parsl_resource_specification"])
