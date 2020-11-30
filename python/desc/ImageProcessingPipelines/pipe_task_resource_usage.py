"""
Resource usage estimates for DRP pipe_tasks as run on a
Cori-Haswell node as of June 2020.
"""
__all__ = ['pipe_tasks']


def nImages(num_visits):
    return 0.77*num_visits


def processCcd():
    return 2.5/60., 1.


def makeCoaddTempExp():
    return 2.6/60., 1.6


def assembleCoadd(num_visits):
    return 0.4*num_visits/60., 1.5


def cpu_mem_visit_scaling(num_visits, cpu0, cpu_index, mem0, mem_index):
    """Return tuple of cpu time (min), max memory (GB)."""
    n_images = nImages(num_visits)
    cpu_time = cpu0*n_images**cpu_index
    mem_max = mem0*n_images**mem_index
    return cpu_time/60., mem_max


def detectCoaddSources(num_visits):
    return cpu_mem_visit_scaling(num_visits, 0.23, 0.78, 0.68, 0.26)


def mergeCoaddDetections():
    return 2.7, 0.7


def deblendCoaddSources(num_visits):
    return cpu_mem_visit_scaling(num_visits, 0.16, 1.40, 0.30, 0.41)


def measureCoaddSources(num_visits):
    return cpu_mem_visit_scaling(num_visits, 1.80, 1.20, 0.43, 0.36)


def mergeCoaddMeasurements():
    return 1, 2.8


def forcedPhotCoadd(num_visits):
    return cpu_mem_visit_scaling(num_visits, 2.20, 1.20, 0.43, 0.36)


pipe_task_funcs = ['processCcd', 'makeCoaddTempExp', 'assembleCoadd',
                   'detectCoaddSources', 'mergeCoaddDetections',
                   'deblendCoaddSources', 'measureCoaddSources',
                   'mergeCoaddMeasurements', 'forcedPhotCoadd']


pipe_tasks = {_: eval(_) for _ in pipe_task_funcs}
