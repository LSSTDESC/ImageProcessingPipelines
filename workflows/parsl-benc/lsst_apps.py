from parsl import bash_app

# This defines a decorator lsst_app which captures the options that
# most of the core application code will need
lsst_app1 = bash_app(executors=["batch-1"],
                    cache=True,
                    ignore_for_cache=["stdout", "stderr", "wrap", "parsl_resource_specification"])
lsst_app2 = bash_app(executors=["batch-2"],
                    cache=True,
                    ignore_for_cache=["stdout", "stderr", "wrap", "parsl_resource_specification"])

