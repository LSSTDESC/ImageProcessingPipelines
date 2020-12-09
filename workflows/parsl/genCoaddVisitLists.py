## genCoaddVisitLists.py - Perform query against the tracts_mapping.sqlite3 database
##
## T.Glanzman - Autumn 2020
__version__ = "1.0.1"

import sys,os
import sqlite3
import argparse
import glob
import logging
logger = logging.getLogger("parsl.dr2.genCoaddVisitLists")


def genCoaddVisitLists(repoDir,dbRerun,visitDir,dbFile,skyCorrRerun,tractID,patchID,filterID,visitMin,visitMax,visitFile,debug=0):
    ## produce visit lists to drive the Coadd processing steps
    if debug > 0: print(f'genCoaddVisitLists: tract {tractID}, patch {patchID}, filter {filterID}')
    ## Prepare DB query
    sql = f"SELECT DISTINCT visit FROM overlaps WHERE tract={tractID} AND filter='{filterID}' AND patch=\'{patchID}\' and visit >= {visitMin} and visit <= {visitMax};"
    if debug > 0:print('sql = ',sql)

    ## Connect to database
    db=os.path.join(repoDir,'rerun',dbRerun,dbFile)
    if debug > 0:print('db = ',db)
    con = sqlite3.connect(db)
    cur = con.cursor()                       ## create a 'cursor'

    ## Query database for all visits (for tract/patch/filter and within a visit range)
    result = cur.execute(sql)
    rows = result.fetchall()   # <-- This is a list of db rows in the result set
    if debug > 1: print('rows = ',rows)
    if debug > 0: print(f'There are {len(rows)} visits')

    ## Prepare visit list
    visitList = []
    lostDetectors = 0
    for row in rows:
        visit = row[0]
        if debug > 0: print('Candidate visit = ',visit)
        
        ## KLUDGE ## Select visits if and only if there are sky
        ## correction data for all relevant sensors

        ## Query database for list of sensors in this visit
        ##   Then check that skyCorrection data exists for each sensor involved
        sql = f"SELECT DISTINCT detector FROM overlaps WHERE tract={tractID} AND filter='{filterID}' AND patch=\'{patchID}\' and visit={visit};"
        result = cur.execute(sql)
        dets = result.fetchall()
        if debug > 0: print('Detectors in this visit = ',dets)
        detList = []
        for line in dets:
            det = f'{line[0]:03}'
            if debug > 1: print('Check if skyCorr for det = ',det)
            skyFilePattern = os.path.join(repoDir,'rerun',skyCorrRerun,'skyCorr',f'{visit:08}-'+filterID,'*','skyCorr_'+f'{visit:08}-'+filterID+'*det'+det+'.fits')
            if debug > 1: print('skyFilePattern = ',skyFilePattern)
            skyFileList = glob.glob(skyFilePattern, recursive=True)
            if debug > 1: print('skyFileList = ',skyFileList)
            ## If skyCorr data exists, keep detector number in a list for later use
            if len(skyFileList) > 0:
                detList.append(det)
            else:
                lostDetectors += 1
                logger.warning('WFLOWq: skyCorr missing for visit/filter/tract/patch/detector: '+'/'.join([str(visit),filterID,tractID,patchID,det]))
                pass
            pass

        ## Keep this visit for processing only if at least one detector has skyCorr data
        if len(detList) > 0:
            detOpt = '^'.join(str(i) for i in detList)
            visitLine = ' '.join([str(visit),detOpt])
            visitList.append(visitLine)
            if debug > 0: print('visitLine = ',visitLine)
        else:
            logger.warning('WFLOWq: Visit has no detectors with skyCorr data: '+str(visit))
            pass
        pass

    ## Generate visit list files:
    ##    1) one visit per line;
    ##    2) one line in --selectID format with visits separated by '^'

    if debug > 0: print('visitList = ',visitList)
    fullVisitFile = os.path.join(repoDir,'rerun',dbRerun,visitDir,visitFile)
    with open(fullVisitFile,'w') as fd:
        if len(visitList) > 0: print("\n".join(str(i) for i in visitList), file=fd)
        pass
    # visitFile2 = fullVisitFile+".selectid"   ## This file contains DM tool option format
    # with open(visitFile2,'w') as fd:
    #     if len(visitList) > 0: print("--selectId visit="+"^".join(str(i) for i in visitList), file=fd)
    #     pass

    if debug > 0: print(f'Total detectors without skyCorr data = {lostDetectors}')
    
    ## All done.
    con.close()
    return 0

    


if __name__ == '__main__':

    ## Parse command line arguments
    parser = argparse.ArgumentParser(description='Perform database query to support DRM workflow.')

    ## Needed inputs: metadata_dir, tract_id, patch_id, filter_id, visit_min, visit_max, visit_file
    parser.add_argument('repoDir',help='Butler repository')
    parser.add_argument('dbRerun',help='repo rerun directory containing DB to query')
    parser.add_argument('dbFile',help='sqlite3 database filename')
    parser.add_argument('skyCorrRerun',help='repo rerun directory containing sky correction data')
    
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
    if args.debug > 1:
        print('command line args: ',args)
        pass

    genCoaddVisitLists(args.repoDir,args.dbRerun,args.dbFile,args.skyCorrRerun,args.tractID,
                       args.patchID,args.filterID,args.visitMin,args.visitMax,args.visitFile,
                       args.debug)
