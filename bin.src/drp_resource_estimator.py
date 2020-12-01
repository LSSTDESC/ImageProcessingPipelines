#!/usr/bin/env python
"""
Script to estimate computing resources for DRP image processing.
"""
import os
import argparse
import sqlite3
import pandas as pd
from desc.ImageProcessingPipelines import extract_coadds, \
    tabulate_pipe_task_resources, total_node_hours


parser = argparse.ArgumentParser(description=('Computing resource estimator '
                                              'for DRP processing'))
parser.add_argument('overlaps_db_file', type=str,
                    help='sqlite3 file containing the overlaps table')
parser.add_argument('--coadd_df_file', type=str, default='coadd_df.pickle',
                    help='pickle file containing the coadd summary dataframe')
parser.add_argument('--knl_factor', type=float, default=8,
                    help='slow-down factor for running on KNL vs Haswell')
parser.add_argument('--verbose', default=True, action='store_false',
                    help='option to enable verbose output')

args = parser.parse_args()

if args.overlaps_db_file.endswith('.pickle'):
    df = pd.read_pickle(args.overlaps_db_file)
else:
    with sqlite3.connect(args.overlaps_db_file) as con:
        df = pd.read_sql('select * from overlaps', con)

# coadds in each band with # visits per coadd:
if not os.path.isfile(args.coadd_df_file):
    coadd_df = extract_coadds(df, verbose=args.verbose)
    coadd_df.to_pickle(args.coadd_df_file)
else:
    coadd_df = pd.read_pickle(args.coadd_df_file)

pt_df = tabulate_pipe_task_resources(df, coadd_df, verbose=args.verbose)

node_hours, node_hours_opt = total_node_hours(pt_df,
                                              cpu_factor=args.knl_factor)

print()
print(pt_df)
print()
print(f'KNL node days: {node_hours/24.:.1f} ({node_hours_opt/24.:.1f})')
