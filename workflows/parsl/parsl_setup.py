import parsl

workers = parsl.ThreadPoolExecutor(max_workers=1)
dfk = parsl.DataFlowKernel(executors=[workers])

#from config.cori import config
#dfk = parsl.DataFlowKernel(config=config)
