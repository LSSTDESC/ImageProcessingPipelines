#!/usr/bin/env python
"""
Script to compute overlaps of sensor-visits with a skymap.
"""
import argparse
import multiprocessing
import numpy as np
import pandas as pd
import lsst.daf.persistence as dp
from desc.ImageProcessingPipelines import SkyMapPolygons, OverlapFinder


description = 'Compute overlaps of sensor-visits with a skymap.'

parser = argparse.ArgumentParser(description=description)
parser.add_argument('--repo', type=str, default=None,
                    help='Data repository containing the skymap object')
parser.add_argument('--opsim_db_file', type=str, default=None,
                    help='OpSim db file')
parser.add_argument('--opsim_constraint', type=str, default=None,
                    help='Selection constraint on Summary table in opsim db')
parser.add_argument('--processes', type=int, default=1,
                    help='Number of processes to run concurrently')
parser.add_argument('--outfile', type=str, default='overlaps.pickle',
                    help='Name of output pickle file to contain the DataFrame')
args = parser.parse_args()

repo = args.repo if args.repo is not None else \
       ('/global/cfs/cdirs/lsst/production/DC2_ImSim/Run2.2i'
        '/desc_dm_drp/v19.0.0-v1/rerun/run2.2i-coadd-wfd-dr6-v1')

opsim_db_file = args.opsim_db_file if args.opsim_db_file is not None else \
                ('/global/cfs/cdirs/descssim/DC2'
                 '/minion_1016_desc_dithered_v4_trimmed.db')

butler = dp.Butler(repo)
skymap = butler.get('deepCoadd_skyMap')
skymap_polygons = SkyMapPolygons(skymap)
overlap_finder = OverlapFinder(opsim_db_file, skymap_polygons)

if args.opsim_constraint is not None:
    df = overlap_finder.opsim_db.query(args.opsim_constraint)
else:
    df = overlap_finder.opsim_db

all_visits = list(df['obsHistID'])

if args.processes == 1:
    df = overlap_finder.get_overlaps(all_visits)
else:
    indexes = np.linspace(0, len(all_visits), args.processes + 1, dtype=int)
    visit_lists = [all_visits[imin:imax] for imin, imax in
                   zip(indexes[:-1], indexes[1:])]

    with multiprocessing.Pool(processes=args.processes) as pool:
        workers = []
        for visits in visit_lists:
            workers.append(pool.apply_async(overlap_finder.get_overlaps,
                                            (visits,)))
        pool.close()
        pool.join()
        dfs = [_.get() for _ in workers]

    df = pd.concat(dfs)

df.to_pickle(args.outfile)
