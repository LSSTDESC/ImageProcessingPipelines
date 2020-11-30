"""
Tabulate the computing resource usage for each DRP pipe_task.
"""
import os
from collections import defaultdict
import sqlite3
import pandas as pd
from .pipe_task_resource_usage import pipe_tasks


__all__ = ['extract_coadds', 'tabulate_pipe_task_resources', 'total_node_time']


def unique_tuples(df, columns):
    return set(zip(*[df[_] for _ in columns]))


def extract_coadds(df, bands='ugrizy', verbose=False):
    data = defaultdict(list)
    for band in bands:
        band_df = df.query(f'filter == "{band}"')
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
    pt_data = defaultdict(list)

    # sensor-visits:
    task_name = 'processCcd'
    sensor_visits = unique_tuples(df, 'visit detector'.split())
    pt_data['pipe_task'].append(task_name)
    num_sensor_visits = len(sensor_visits)
    pt_data['num_instances'].append(num_sensor_visits)
    cpu_time, mem_max = pipe_tasks[task_name]()
    pt_data['cpu_time'].append(cpu_time*num_sensor_visits)
    pt_data['mem_max'].append(mem_max)

    # warps:
    task_name = 'makeCoaddTempExp'
    warps = unique_tuples(df, 'visit tract patch'.split())
    pt_data['pipe_task'].append(task_name)
    num_warps = len(warps)
    pt_data['num_instances'].append(num_warps)
    cpu_time, mem_max = pipe_tasks[task_name]()
    pt_data['cpu_time'].append(cpu_time*num_warps)
    pt_data['mem_max'].append(mem_max)

    task_names = ('assembleCoadd', 'detectCoaddSources', 'deblendCoaddSources',
                  'measureCoaddSources', 'forcedPhotCoadd')
    for task_name in task_names:
        if verbose:
            print("processing", task_name)
        pt_data['pipe_task'].append(task_name)
        pt_data['num_instances'].append(len(coadd_df))
        cpu_time_total = 0
        mem_max = 0
        for i, row in coadd_df.iterrows():
            num_visits = row['num_visits']
            cpu_time, mem = pipe_tasks[task_name](num_visits)
            cpu_time_total += cpu_time
            mem_max = max(mem_max, mem)
        pt_data['cpu_time'].append(cpu_time_total)
        pt_data['mem_max'].append(mem_max)
    return pd.DataFrame(data=pt_data)


def total_node_time(pt_df, cpu_factor=8, cores_per_node=68,
                    memory_per_node=96, memory_min=10):
    available_memory = memory_per_node - memory_min
    node_time = 0
    for i, row in pt_df.iterrows():
        ncores = min(cores_per_node, int(available_memory/row['mem_max']))
        node_time += row['cpu_time']*cpu_factor/ncores
    return node_time
