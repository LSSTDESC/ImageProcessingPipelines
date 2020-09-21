## genCoaddVisitLists.py - Perform query against the tracts_mapping.sqlite3 database
##
## T.Glanzman - Autumn 2020
__version__ = "1.0.0"

import sys,os
import sqlite3
import argparse
import glob
import logging
logger = logging.getLogger("parsl.workflow")


def genCoaddVisitLists(repoDir,dbDir,dbFile,skyCorrDir,tractID,patchID,filterID,visitMin,visitMax,visitFile,debug=False):
    ## produce two coadd visit lists
    
    ## Prepare DB query
    sql = f"SELECT DISTINCT visit FROM overlaps WHERE tract={tractID} AND filter='{filterID}' AND patch=\'{patchID}\' and visit >= {visitMin} and visit <= {visitMax};"
    if debug > 0:print('sql = ',sql)

    ## Connect to database
    db=os.path.join(repoDir,'rerun',dbDir,dbFile)
    if debug > 0:print('db = ',db)
    con = sqlite3.connect(db)
    cur = con.cursor()                       ## create a 'cursor'

    ## Perform database query
    result = cur.execute(sql)
    rows = result.fetchall()   # <-- This is a list of db rows in the result set
    if debug > 0: print('rows = ',rows)

    ## Prepare visit list
    visitList = []
    for row in rows:
        visit = row[0]
        if debug > 0: print('Candidate visit = ',visit)
        ## KLUDGE ## Select visits if and only if there are sky correction data for all relevant sensors
        ## Query database for list of sensors
        sql = f"SELECT DISTINCT detector FROM overlaps WHERE tract={tractID} AND filter='{filterID}' AND patch=\'{patchID}\' and visit={visit};"
        result = cur.execute(sql)
        dets = result.fetchall()
        if debug > 0: print('Detectors in this visit = ',dets)
        keep = True
        for line in dets:
            det = f'{line[0]:03}'
            if debug > 0: print('type(det) = ',type(det))
            if debug > 0: print('Check if skyCorr for det = ',det)
            skyFilePattern = os.path.join(repoDir,'rerun',skyCorrDir,'skyCorr',f'{visit:08}-'+filterID,'*','skyCorr_'+f'{visit:08}-'+filterID+'*det'+det+'.fits')
            if debug > 0: print('skyFilePattern = ',skyFilePattern)
            skyFileList = glob.glob(skyFilePattern, recursive=True)
            if debug > 0: print('skyFileList = ',skyFileList)
            if len(skyFileList) <= 0:
                keep = False
                logging.warning('WFLOWq: skyCorr missing for visit/filter/tract/patch/detector: '+'/'.join([str(visit),filterID,tractID,patchID,det]))
            pass
        if debug > 0: print('visit ',visit,', keep = ',keep)
        if keep: visitList.append(visit)
        pass

    ## Prepare two visit list files:
    ##    1) one visit per line;
    ##    2) one line in --selectID format with visits separated by '^'

    if debug > 0: print('visitList = ',visitList)
    fullVisitFile = os.path.join(repoDir,'rerun',dbDir,visitFile)
    with open(fullVisitFile,'w') as fd:
        print("\n".join(str(i) for i in visitList), file=fd)
        pass
    visitFile2 = fullVisitFile+".selectid"
    with open(visitFile2,'w') as fd:
        print("--selectId visit="+"^".join(str(i) for i in visitList), file=fd)
        pass
    return

    


if __name__ == '__main__':

    ## Parse command line arguments
    parser = argparse.ArgumentParser(description='Perform database query to support DRM workflow.')

    ## Needed inputs: metadata_dir, tract_id, patch_id, filter_id, visit_min, visit_max, visit_file
    parser.add_argument('repoDir',help='Butler repository')
    parser.add_argument('dbDir',help='repo rerun directory containing DB to query')
    parser.add_argument('dbFile',help='sqlite3 database filename')
    parser.add_argument('skyCorrDir',help='repo rerun directory containing sky correction data')
    
    parser.add_argument('tractID',help='Tract')
    parser.add_argument('patchID',help='Patch')
    parser.add_argument('filterID',help='Filter')
    parser.add_argument('visitMin',help='Earlist visit')
    parser.add_argument('visitMax',help='Latest visit')
    parser.add_argument('visitFile',help='File to contain results')
    

    
    # parser.add_argument('-f','--file',default='./monitoring.db',help='name of Parsl monitoring database file (default=%(default)s)')
    # parser.add_argument('-r','--runnum',type=int,help='Specific run number of interest (default = latest)')
    # parser.add_argument('-s','--schemas',action='store_true',default=False,help="only print out monitoring db schema for all tables")
    # parser.add_argument('-t','--taskID',default=None,help="specify task_id (taskHistory only)")
    # parser.add_argument('-S','--taskStatus',default=None,help="specify task_status_name")
    # parser.add_argument('-l','--taskLimit',type=int,default=0,help="limit output to N tasks (default is no limit)")
    parser.add_argument('-d','--debug',type=int,default=0,help='Set debug level (default = %(default)s)')

    parser.add_argument('-v','--version', action='version', version=__version__)
    print(sys.version)
    
    args = parser.parse_args()
    if args.debug > 0:
        print('command line args: ',args)
        pass

    genCoaddVisitLists(args.repoDir,args.dbDir,args.dbFile,args.skyCorrDir,args.tractID,
                       args.patchID,args.filterID,args.visitMin,args.visitMax,args.visitFile,
                       args.debug)
