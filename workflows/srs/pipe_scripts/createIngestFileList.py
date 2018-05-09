#!/usr/bin/env python


"""Create a list of files to ingest."""


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
    parser.add_argument("--maxfiles", default=500000,
                        help="Maximum number of files per output.")
    parser.add_argument("--increment", action="store_true", default=False,
                        help="Check in the current directory if there are files with data"
                        " and only output what is not already in those files.")
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

    if args.increment:
        existing_ftis = glob("filesToIngest*.txt")
        existing_files = np.concatenate([np.loadtxt(f, dtype='string', unpack=True)
                                         for f in existing_ftis])
        print("%i paths to files found" % len(existing_files))
        files = [f for f in files if f not in existing_files]
        print("%i new files to ingest will be saved" % len(files))
    else:
        existing_ftis = []

    # Do we have more than the maximum number of file?
    if not len(files) > args.maxfiles:
        # Save the list of files
        if not args.filename.endswith('.txt'):
            args.filename += '.txt'
        print("Saving the list of files in ", args.filename)
        np.savetxt(args.filename, files, fmt="%s")
    else:
        # Save the list of files
        if not args.filename.endswith('.txt'):
            args.filename += '.txt'
        file_lists = [files[i: i + args.maxfiles]
                      for i in range(0, len(files), args.maxfiles)]
        for i, files in enumerate(file_lists):
            filename = args.filename.replace(".txt", "_%i.txt" % (i + len(existing_ftis)))
            print("Saving the list of files in ", filename)
            np.savetxt(filename, files, fmt="%s")
