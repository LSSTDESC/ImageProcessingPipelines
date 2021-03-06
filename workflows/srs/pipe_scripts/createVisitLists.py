#!/usr/bin/env python


"""Build the list of dataIds for all filters"""


from __future__ import print_function
import os
import sys
from random import shuffle
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
import lsst.daf.persistence as dafPersist
import numpy as np

def get_dataIds(catalog):
    """Get the list of existing dataids for the 'raw' or 'eimage' and 'calexp' catalogs."""
    # Get all available keys for the given datasetType
    keys = butler.getKeys(catalog)
    #there is no guaranty that filter is in the returned keys
    if 'filter' not in keys.keys():
        keys['filter']=str
    # Construct and return the dataIds dictionnary for all available data
    metadata = butler.queryMetadata(catalog, format=sorted(keys.keys()))
    dataids = [dict(zip(sorted(keys.keys()), list(v)
                        if not isinstance(v, list) else v))
               for v in metadata]
    return [dataid for dataid in dataids if butler.datasetExists(catalog, dataid)]


def compare_dataIds(dataIds_1, dataIds_2):
    """Compare two list of dataids.

    Return a list of dataIds present in 'raw' or 'eimage' (1) but not in 'calexp' (2).
    """
    print("INFO %i input dataIds found" % len(dataIds_1))
    print("INFO %i calexp dataIds found" % len(dataIds_2))
    dataIds_1 = [{k: v for k, v in d.items() if k != 'snap'} for d in dataIds_1]
    return [dataid for dataid in dataIds_1 if dataid not in dataIds_2]


if __name__ == "__main__":

    usage = """%s [options] input""" % __file__
    description = """Build the list of data ids for all filters."""

    parser = ArgumentParser(usage=usage, description=description,
                            formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('input', help="Path to the input butler folder.")
    parser.add_argument('--type', default='raw',help="type is raw or eimage.")
    parser.add_argument('--output', help="Path to dir where visit files are written.",default=os.path.curdir)
    parser.add_argument('--increment', action='store_true',
                        help="Only keep data ids not yet processed.")
    parser.add_argument("--idopt", default='id', help="id option to put in front "
                        "of the visit name. Could be 'selectId' or 'id'")
    args = parser.parse_args()

    if not os.path.exists(args.input):
        raise IOError("Input directory does not exists")
    if args.idopt not in ['selectId', 'id']:
        raise IOError("Option idopt must be 'selectid' or 'id'")    

    dummy = os.system('mkdir -p %s'%args.output)
    
    # Load the butler for this input directory
    datadir=args.input
    print("butler call on dir %s"%datadir)
    butler = dafPersist.Butler(datadir)
    
    if args.increment:
        print("INFO: Checking for dataIds that haven't been processed yet.")
        # Only keep visit that haven't been processed yet (no calexp data)
        dataids = compare_dataIds(get_dataIds(args.type), get_dataIds('calexp'))
    else:
        # Process all visit found in the input directories
        dataids = get_dataIds(args.type)
    print("INFO %i (new) dataIds to process found" % len(dataids))

    # Get the list of available filters
    filters = set([dataid['filter'] for dataid in dataids])
    print("INFO: Working on %i filters:" % len(filters), filters)

    # Dictionnary of dataIds per filter
    fdataids = {filt: [dataid for dataid in dataids if dataid['filter'] == filt]
                for filt in filters}
    fvisits = {}
    for filt in filters:
        vislist = [dataid['visit'] for dataid in dataids if dataid['filter'] == filt]
        visits = np.array(vislist, dtype=int)
        fvisits[filt]=np.unique(visits)


    # Do we have (new) dataIds to process? 
    if not any([len(fdataids[filt]) for filt in filters]):
        print("No (new) dataIds to process. Exit.")
        sys.exit(0)

    # We do have visit to process
    print("The total number of dataids is", sum([len(fdataids[filt]) for filt in fdataids]))
    print("The number of data Ids per filter are:")
    for filt in sorted(fdataids):
        print(" - %s: %i" % (filt, len(fdataids[filt])))

    # Write and save the lists
    print("Wrinting visit list in separated files for each filter")
    for filt in fdataids:
        shuffle(fdataids[filt])  # in case we have to re-run things, the order will not always be the same

        visit_file = os.path.join(args.output, "%s_visit.list" % filt)
        file_to_save = open(visit_file, 'w')
        for v in fvisits[filt]:
            file_to_save.write("--%s visit=%i\n" %(args.idopt, v))
        file_to_save.close()

        all_file = os.path.join(args.output, "%s.list" % filt)
        file_to_save = open(all_file, 'w')
        for dataid in fdataids[filt]:
            try:
                sensor_name='detector'
                sensor=dataid[sensor_name]
            except:
                try:
                    sensor_name='sensor'
                    sensor=dataid[sensor_name]
                except:
                    sensor_name='ccd' 
                    sensor=dataid[sensor_name]
            file_to_save.write("--%s visit=%i raft=%s %s=%s\n" % \
                               (args.idopt, dataid['visit'], dataid['raft'], sensor_name, sensor))
        file_to_save.close()
        print(" - %s: %i dataids -> %s" %(filt, len(fdataids[filt]), visit_file))

