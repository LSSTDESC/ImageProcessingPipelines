"""
Tabulate the computing resource usage for each DRP pipe_task.
"""
from collections import defaultdict
import numpy as np
import pandas as pd
from .pipe_task_resource_usage import pipe_tasks


__all__ = ['extract_coadds', 'tabulate_pipe_task_resources',
           'total_node_hours']


def unique_tuples(df, columns):
    """
    Return the set of unique tuples from a dataframe for the specified
    columns.

    Parameters
    ----------
    df : pandas.DataFrame
        Dataframe with the columns of data to be considered.
    columns : list-like
        A list of column names in the dataframe from which to
        construct the tuples.
    """
    return set(zip(*[df[_] for _ in columns]))


def extract_coadds(df, bands='ugrizy', verbose=False):
    """
    Extract the coadds for each band-tract-patch combination, and
    compute the number of visits per coadd for resource scaling.

    Parameters
    ----------
    df : pandas.DataFrame
        Dataframe containing the overlaps table information, i.e.,
        overlap of sensor-visits with patches in the skymap.
    bands : list-like ['ugrizy']
        Bands to consider, e.g., the standard ugrizy bands for Rubin.
    verbose : bool [False]
        Verbosity flag.

    Returns
    -------
    pandas.DataFrame with the band, tract, patch, num_visits columns.
    """
    band_colname = 'band' if 'band' in df else 'filter'
    data = defaultdict(list)
    for band in bands:
        band_df = df.query(f'{band_colname} == "{band}"')
        tracts = set(band_df['tract'])
        for i, tract in enumerate(tracts):
            tract_df = band_df.query(f'tract == {tract}')
            if verbose:
                print(i, band, tract, len(tracts))
            patches = set(tract_df['patch'])
            for patch in patches:
                my_df = tract_df.query(f'patch == "{patch}"')
                data['band'].append(band)
                data['tract'].append(tract)
                data['patch'].append(patch)
                data['num_visits'].append(len(set(my_df['visit'])))
    return pd.DataFrame(data=data)


def tabulate_pipe_task_resources(df, coadd_df, verbose=False):
    """
    Tabulate the computing resources (cpu time, memory) for each
    of the pipe_tasks given a dataframe with the overlaps information.

    Parameters
    ----------
    df : pandas.DataFrame
        Dataframe containing the overlaps table information, i.e.,
        overlap of sensor-visits with patches in the skymap.
    coadd_df : pandas.DataFrame
        Dataframe with the number of vists for each band-tract-patch
        combination.  This is the output of `extract_coadds(df, ...)`.
    verbose : bool [False]
        Verbosity flag.

    Returns
    -------
    pandas.DataFrame with the number of instances, total cpu time,
    and maximum and average memory used per pipe_task.
    """
    pt_data = defaultdict(list)

    # sensor-visits:
    task_name = 'processCcd'
    if verbose:
        print("processing", task_name)
    pt_data['pipe_task'].append(task_name)
    num_sensor_visits = len(unique_tuples(df, 'visit detector'.split()))
    pt_data['num_instances'].append(num_sensor_visits)
    cpu_hours, mem_GB = pipe_tasks[task_name]()
    pt_data['cpu_hours'].append(cpu_hours*num_sensor_visits)
    pt_data['max_GB'].append(mem_GB)
    pt_data['avg_GB'].append(mem_GB)

    # warps:
    task_name = 'makeCoaddTempExp'
    if verbose:
        print("processing", task_name)
    pt_data['pipe_task'].append(task_name)
    num_warps = len(unique_tuples(df, 'visit tract patch'.split()))
    pt_data['num_instances'].append(num_warps)
    cpu_hours, mem_GB = pipe_tasks[task_name]()
    pt_data['cpu_hours'].append(cpu_hours*num_warps)
    pt_data['max_GB'].append(mem_GB)
    pt_data['avg_GB'].append(mem_GB)

    for task_name in ('assembleCoadd', 'detectCoaddSources',
                      'deblendCoaddSources', 'measureCoaddSources',
                      'forcedPhotCoadd'):
        if verbose:
            print("processing", task_name)
        pt_data['pipe_task'].append(task_name)
        pt_data['num_instances'].append(len(coadd_df))
        cpu_hours_total = 0
        memory = []
        for _, row in coadd_df.iterrows():
            num_visits = row['num_visits']
            cpu_hours, mem_GB = pipe_tasks[task_name](num_visits)
            cpu_hours_total += cpu_hours
            memory.append(mem_GB)
        pt_data['cpu_hours'].append(cpu_hours_total)
        pt_data['max_GB'].append(np.max(memory))
        pt_data['avg_GB'].append(np.mean(memory))

    for task_name in ('mergeCoaddDetections', 'mergeCoaddMeasurements'):
        if verbose:
            print("processing", task_name)
        pt_data['pipe_task'].append(task_name)
        num_instances = int(len(coadd_df)/6)
        pt_data['num_instances'].append(num_instances)
        cpu_hours, mem_GB = pipe_tasks[task_name]()
        pt_data['cpu_hours'].append(num_instances*cpu_hours)
        pt_data['max_GB'].append(mem_GB)
        pt_data['avg_GB'].append(mem_GB)

    return pd.DataFrame(data=pt_data)


def total_node_hours(pt_df, cpu_factor=8, cores_per_node=68,
                     memory_per_node=96, memory_min=10):
    """
    Estimate the total number of node hours to do an image processing
    run.

    Parameters
    ----------
    pt_df : pandas.DataFrame
        DataFrame containing the number of instances, total cpu time,
        and maximum and average memory used per pipe_task.  This is
        the output of `tabulate_pipe_task_resources`.
    cpu_factor : float [8]
        Slow down factor to apply to the pipe_task cpu times.  The
        cpu times were derived from Cori-Haswell runs and the default
        value of 8 is the empirically observed slow-down for running
        the same cpu-bound job on a Cori-KNL node.
    cores_per_node : int [68]
        Number of cores per node.  68 is the Cori-KNL value.
    memory_per_node : int [96]
        Memory per node in GB.  96 is the Cori-KNL value.
    memory_min : int [10]
        Memory in GB to reserve per node as a safety factor.  10GB is
        a conservative number for these jobs.

    Returns
    -------

    tuple(float, float): The first entry, `node_hours`, is computed
    using the maximum memory estimate per process to determine the
    number of cores per node for a given pipe task ; the second entry,
    `node_hours_opt`, is an optimistic estimate using the average
    memory per process.
    """
    available_memory = memory_per_node - memory_min
    node_hours = 0
    node_hours_opt = 0
    for _, row in pt_df.iterrows():
        ncores = min(cores_per_node, int(available_memory/row['max_GB']))
        ncores_avg = min(cores_per_node, int(available_memory/row['avg_GB']))
        node_hours += row['cpu_hours']*cpu_factor/ncores
        node_hours_opt += row['cpu_hours']*cpu_factor/ncores_avg
    return node_hours, node_hours_opt
