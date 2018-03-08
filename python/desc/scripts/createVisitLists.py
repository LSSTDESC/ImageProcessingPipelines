"""Build the list of visits for all filters"""


from __future__ import print_function
import os
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
import lsst.daf.persistence as dafPersist


if __name__ == "__main__":

    usage = """%s [options] input""" % __file__
    description = """Build the list of visits for all filters."""

    parser = ArgumentParser(usage=usage, description=description,
                            formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('input', help="Path to the input butler folder.")
    args = parser.parse_args()

    if not os.path.exists(args.input):
        raise IOError("Input directory does not exists")
    if not args.input.endswith('/'):
        args.input += '/'

    # Load the butler for this input directory
    butler = dafPersist.Butler(args.input)

    # Get all available keys for the 'raw' datasetType
    catalog = 'raw'
    keys = butler.getKeys(catalog)

    # Construct the dataIds dictionnary for all available data
    metadata = butler.queryMetadata(catalog, format=sorted(keys.keys()))
    dataids = [dict(zip(sorted(keys.keys()), list(v) if not isinstance(v, list) else v))
               for v in metadata]

    # Get the list of available filters
    filters = set([dataid['filter'] for dataid in dataids])

    # Dictionnary of visits per filter
    visits = {filt: list(set([dataid['visit'] for dataid in dataids if dataid['filter'] == filt]))
              for filt in filters}

    # Print some info
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

