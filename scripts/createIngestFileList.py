from __future__ import print_function
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from glob import glob
import numpy as np

if __name__ == "__main__":

    usage = """%s [options] inputdir""" % __file__
    description = """Create a list of files to ingest."""

    parser = ArgumentParser(usage=usage, description=description,
                            formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputdir',
                        help="Path to folder containing the raw data.")
    parser.add_argument("--ext", default='.fits.gz',
                        help="Extension of the files to ingest.")
    parser.add_argument("--recursive", action="store_true", default=False,
                        help="Recursively look for files to ingest.")
    parser.add_argument("--filename", default='filesToIngest.txt',
                        help="Name of the output (.txt) file.")
    args = parser.parse_args()

    # Make sure that the given extension starts with a '.'
    if not args.ext.startswith('.'):
        args.ext = '.' + args.ext

    # Is it a recursive search of not
    if args.recursive:
        args.inputdir += '/**'

    # Add the file extension to the input directory path
    args.inputdir += '/*' + args.ext

    # Get the list of files
    files = glob(args.inputdir, recursive=args.recursive)
    print("%i files found in" % len(files),
          args.inputdir, 'with extension', args.ext)

    # Save the list of files
    if not args.filename.endswith('.txt'):
        args.filename += '.txt'
    print("Saving the list of files in ", args.filename)
    np.savetxt(args.filename, files, fmt="%s")
