#!/usr/bin/env python


"""Build the list of visits for all filters"""


from __future__ import print_function
import os
import sys
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
import lsst.daf.persistence as dafPersist


def get_dataIds(catalog):
    """Get the list of existing dataids for the 'raw' and 'calexp' catalogs."""
    # Get all available keys for the given datasetType
    keys = butler.getKeys(catalog)
    # Construct and return the dataIds dictionnary for all available data
    metadata = butler.queryMetadata(catalog, format=sorted(keys.keys()))
    return [dict(zip(sorted(keys.keys()), list(v)
                     if not isinstance(v, list) else v))
            for v in metadata]


def compare_dataIds(dataIds_1, dataIds_2):
    """Compare two list of dataids.

    Return a list of dataIds present in 'raw' (1) but not in 'calexp' (2).
    """
    visits_1 = [dataid['visit'] for dataid in dataIds_1]
    visits_2 = [dataid['visit'] for dataid in dataIds_2]
    visits_to_keep = [visit for visit in visits_1 if visit not in visits_2]
    return [dataid for dataid in dataIds_1 if dataid['visit'] in visits_to_keep]


if __name__ == "__main__":

    usage = """%s [options] input""" % __file__
    description = """Build the list of visits for all filters."""

    parser = ArgumentParser(usage=usage, description=description,
                            formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('input', help="Path to the input butler folder.")
    parser.add_argument('--increment', action='store_true',
                        help="Only keep visits not yet processed.")
    args = parser.parse_args()

    if not os.path.exists(args.input):
        raise IOError("Input directory does not exists")
    if not args.input.endswith('/'):
        args.input += '/'

    # Load the butler for this input directory
    butler = dafPersist.Butler(args.input)

    if args.increment:
        # Only keep visit that haven't been processed yet (no calexp data)
        dataids = compare_dataIds(get_dataIds('raw'), get_dataIds('calexp'))
    else:
        # Process all visit found in the input directories
        dataids = get_dataIds('raw')

    # Get the list of available filters
    filters = set([dataid['filter'] for dataid in dataids])

    # Dictionnary of visits per filter
    visits = {filt: list(set([dataid['visit'] for dataid in dataids if dataid['filter'] == filt]))
              for filt in filters}

    # Do we have (new) visits to process? 
    if len(visits) == 0:
        print("No (new) visits to process. Exit.")
        sys.exit(0)

    # We do have visit tp process
    print("The total number of visits is", sum([len(visits[filt]) for filt in visits]))
    print("The number of visits per filter are:")
    for filt in sorted(visits):
        print(" - %s: %i" % (filt, len(visits[filt])))

    # Write and save the lists
    print("Wrinting visit list in separated files for each filter")
    for filt in visits:
        visit_file = "%s.list" % filt
        file_to_save = open(visit_file, 'w')
        for visit in visits[filt]:
            file_to_save.write("--id visit=%s\n" % visit)
        file_to_save.close()
        print(" - %s: %i visits -> %s" %(filt, len(visits[filt]), visit_file))

