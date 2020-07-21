from parsl import bash_app

lsst_app1 = bash_app(executors=["batch-1"],
                     cache=True,
                     ignore_for_cache=["stdout", "stderr", "wrap", "parsl_resource_specification"])

lsst_app2 = bash_app(executors=["batch-2"],
                     cache=True,
                     ignore_for_cache=["stdout", "stderr", "wrap", "parsl_resource_specification"])
