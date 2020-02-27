## wstat.py - workflow status summary from Parsl monitoring database

## The idea is not to replace the "sqlite3" interactive command or the
## Parsl web interface, but to complement them to create some useful
## interactive summaries specific to Parsl workflows.

## Python dependencies: sqlite3, tabulate

## T.Glanzman - Spring 2019
__version__ = "0.8.5"
pVersion='0.9.0'    ## Parsl version

import sys,os
import sqlite3
from tabulate import tabulate
import datetime
import argparse

## Table format is used by 'tabulate' to select the exact output format
## 'grid' looks nice but is non-compact
## 'psql' looks almost as nice and is more compact
tblfmt = 'psql'

class pmon:
    ### class pmon - interpret Parsl monitoring database
    def __init__(self,dbfile='monitoring.db'):
        ## Instance variables
        self.dbfile = dbfile
        self.debug = False

        ## sqlite3 database connection and cursor
        self.con = sqlite3.connect(self.dbfile,
                                   detect_types=sqlite3.PARSE_DECLTYPES |
                                   sqlite3.PARSE_COLNAMES)      ## connect to sqlite3 file
#        self.con = sqlite3.connect(self.dbfile)      ## connect to sqlite3 file
        self.con.row_factory = sqlite3.Row           ## optimize output format
        self.cur = self.con.cursor()                       ## create a 'cursor'

        ## Read in the workflow (summary) table
        self.wrows = None
        self.wtitles = None
        self.runid2num = None
        self.runnum2id = None
        self.numRuns = 0
        self.runmin = 999999999
        self.runmax = -1
        self.readWorkflowTable()
        return


    def __del__(self):
        ## Class destructor 
        self.con.close()

    def stripms(self,intime):
        ## Trivial function to strip off millisec from Parsl time string
        return datetime.datetime.strptime(str(intime),'%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%d %H:%M:%S')

    def readWorkflowTable(self,sql="select * from workflow"):
        ## Extract all rows from 'workflow' table
        ## workflow table:  ['run_id', 'workflow_name', 'workflow_version', 'time_began', 
        ##                           'time_completed', 'workflow_duration', 'host', 'user', 'rundir', 
        ##                           'tasks_failed_count', 'tasks_completed_count']
        ##
        ## This alternate query returns a list of one 'row' containing the most recent entry
        #sql = "select * from workflow order by time_began desc limit 1"
        (self.wrows,self.wtitles) = self.stdQuery(sql)
        self.runid2num = {}
        self.runnum2id = {}
        for row in self.wrows:
            runID = row['run_id']
            runNum = os.path.basename(row['rundir'])
            self.runid2num[runID] = runNum
            self.runnum2id[int(runNum)] = runID
            if int(runNum) > self.runmax: self.runmax = int(runNum)
            if int(runNum) < self.runmin: self.runmin = int(runNum)
            pass
        self.numRuns = len(self.wrows)
        # print('runid2num = ',self.runid2num)
        # print('runnum2id = ',self.runnum2id)
        # print('runmin = ',self.runmin)
        # print('runmax = ',self.runmax)
        return


    def getTableList(self):
        ## Fetch list of all tables in this database
        ## Parsl monitoring.db currently contains four tables: resource, status, task, workflow
        self.cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        rawTableList = self.cur.fetchall()
        tableList = []
        for table in rawTableList:
            tableList.append(table[0])
            pass
        return tableList


    def getTableSchema(self,table='all'):
        ## Fetch the schema for one or more tables
        if table == 'all':
            sql = "select sql from sqlite_master where type = 'table' ;"
        else:
            sql = "select sql from sqlite_master where type = 'table' and name = '"+table+"';"
        self.cur.execute(sql)
        schemas = self.cur.fetchall()
        return schemas


    def printRow(self,titles,row):
        ## Pretty print one row with associated column names
        for title,col in zip(titles,row):
            print(title[0],": ",col)
            pass
        pass
        return



    def dumpResults(self,titles,rowz):
        ## Pretty print all rows with column names
        print("\ndumpResults:\n")
        print("titles = ",titles)
        for row in rowz:
            for key in row.keys():
                print(row[key])
            pass
        print("-------------end of dump--------------")
        return
    

    def stdQuery(self,sql):
        ## Perform a query, fetch all results and column headers
        result = self.cur.execute(sql)
        rows = result.fetchall()   # <-- This is a list of db rows in the result set
        ## This will generate a list of column headings (titles) for the result set
        titlez = result.description
        ## Convert silly 7-tuple title into a single useful value
        titles = []
        for title in titlez:
            titles.append(title[0])
            pass
        if self.debug:
            print("titles = ",titles)
            print("rows = ",rows)
            pass
        return (rows,titles)


    def selectRunID(self,runnum=None):
        ## Select the workflow table row based on the requested runNumber (not to be confused with run_id)

        if runnum == None:         # Select most recent workflow run
            #print("runnum = None, returning -1")
            return -1
        else:
            for rdx in list(range(len(self.wrows))):
                if self.wrows[rdx]['run_id'] == self.runnum2id[runnum]:
                    #print("runnum = ",runnum,', workflow table row = ',rdx)
                    return rdx
                pass
            assert False,"Help!"
            pass
        pass


    def printWorkflowSummary(self,runnum=None):
        ## Summarize current state of workflow
        repDate = datetime.datetime.now()
        titles = self.wtitles

        ##  Select desired workflow 'run'
        nRuns = self.numRuns
        rowindex = self.selectRunID(runnum)
        row = self.wrows[rowindex]

        runNum = os.path.basename(row['rundir'])
        runNumTxt = runNum
        irunNum = int(runNum)
        if irunNum == int(self.runmax):runNumTxt += '    <<-most current run->>'
        runID = row['run_id']
        exeDir = os.path.dirname(os.path.dirname(row['rundir']))

        completedTasks = row['tasks_completed_count']+row['tasks_failed_count']

        runStart = row['time_began']
        if runStart == None:
            runStart = '*pending*'
        else:
            runStart = self.stripms(runStart)
            pass

        runEnd   = row['time_completed']
        #print('runEnd [',type(runEnd),'] = ',runEnd)
        if runEnd == None:
            runEnd = '*pending*'
        else:
            runEnd = self.stripms(runEnd)
            pass
        
        duration = row['workflow_duration']
        if duration == None:
            duration = '*pending*'
        else:
            duration = datetime.timedelta(seconds=int(row['workflow_duration']))
            pass


        ##   Print SUMMARIES
        print('Workflow summary\n================')
        wSummaryList = []
        wSummaryList.append(['Report Date/Time ',repDate ])
        wSummaryList.append(['workflow name',row['workflow_name']])
        wSummaryList.append(['run',runNumTxt ])
        wSummaryList.append(['user', row['user']])
        wSummaryList.append(['MonitorDB',self.dbfile])
        wSummaryList.append(['workflow rundir',exeDir])
        wSummaryList.append(['workflow node', row['host']])
        wSummaryList.append(['run start',runStart ])
        wSummaryList.append(['run end ',runEnd ])
        wSummaryList.append(['run duration ', duration])
        wSummaryList.append(['tasks completed',completedTasks ])
        wSummaryList.append(['tasks completed: success', row['tasks_completed_count']])
        wSummaryList.append(['tasks completed: failed',row['tasks_failed_count'] ])
        print(tabulate(wSummaryList, tablefmt=tblfmt))
        return
        



    def printTaskSummary(self,runnum=None,opt=None):
        ## The task summary is a composite presentation of values from
        ## the 'task' and 'status' tables

        ##  Select requested Run in workflow table
        rowindex = self.selectRunID(runnum)
        wrow = self.wrows[rowindex]
        if runnum == None: runnum = int(self.runid2num[wrow['run_id']])

        ##  Header
        header = '\n\nTask summary for run '+str(runnum)
        if runnum == int(self.runmax):header += ' [most current run]'
        print(header,'\n===========================================')

        ##  Query the 'task' table for data to present
        runID = wrow['run_id']
        sql = 'select task_id,task_func_name,hostname,task_fail_count,task_time_submitted,task_time_running,task_time_returned,task_elapsed_time,task_stdout from task where run_id = "'+wrow['run_id']+'" order by task_id asc'
        (tRowz,tTitles) = self.stdQuery(sql)

        
        ## Convert from sqlite3.Row to a simple 'list'
        tRows = []
        
        ## Adjust data for presentation
        logDir = 'not specified'
        if tRowz[0]['task_stdout'] != None: logDir = os.path.dirname(tRowz[0]['task_stdout'])
        stdoutIndx = tTitles.index('task_stdout')
        elapsedIndx = tTitles.index('task_elapsed_time')
        subTimeIndx = tTitles.index('task_time_submitted')
        startTimeIndx = tTitles.index('task_time_running')
        endTimeIndx = tTitles.index('task_time_returned')
        
        for rw in tRowz:
            tRows.append(list(rw))
            if tRows[-1][stdoutIndx] != None:
                tRows[-1][stdoutIndx] = os.path.basename(tRows[-1][stdoutIndx])  ## Remove stdout file path
            if tRows[-1][elapsedIndx] != None:
                a = datetime.timedelta(seconds=int(tRows[-1][elapsedIndx]))
                tRows[-1][elapsedIndx] = str(a)
                pass

            for ix in [subTimeIndx,startTimeIndx,endTimeIndx]:
                if tRows[-1][ix] != None:
                    tRows[-1][ix] = self.stripms(tRows[-1][ix])
                    pass
                pass

            ## Calculate run duration (wall clock time while running)
            runDuration=''
            startTime = tRows[-1][startTimeIndx]
            endTime = tRows[-1][endTimeIndx]
            startDT=None
            endDT=None
            if startTime != None:
                startDT = datetime.datetime.strptime(str(tRows[-1][startTimeIndx]),'%Y-%m-%d %H:%M:%S')
            if endTime != None:
                endDT = datetime.datetime.strptime(str(tRows[-1][endTimeIndx]),'%Y-%m-%d %H:%M:%S')
            if startDT != None and endDT != None: runDuration = endDT-startDT
            #print('runDuration = ',runDuration,'   (',type(runDuration),')')
            tRows[-1][elapsedIndx] = str(runDuration)
            pass
            

        ## Construct summary
        numTasks = len(tRows)
        durationSec = wrow['workflow_duration']
        if durationSec == None:
            print('workflow script has not reported completion, i.e., running, crashed, or killed')
        else:
            duration = datetime.timedelta(seconds=int(durationSec))
            print('workflow elapsed time = ',duration,' (hh:mm:ss)')
            print('number of Tasks launched = ',numTasks)
            if numTasks == 0:return
            pass

        ## Extract status data from 'status' table
        tTitles.insert(2, "status")
        tStat = {'pending':0,'launched':0,'runnable':0,'running':0,'retry':0,'unsched':0,'unknown':0,'done':0,'failed':0,'dep_fail':0}
        for row in range(numTasks):
            taskID = tRows[row][0]
            #print('runID = ',runID,', taskID = ',taskID)
            sql = 'select task_id,timestamp,task_status_name from status where run_id="'+str(runID)+'" and task_id="'+str(taskID)+'" order by timestamp desc limit 1'
            (sRowz,sTitles) = self.stdQuery(sql)
            if self.debug: self.dumpResults(sTitles,sRowz)
            taskStat = sRowz[0]['task_status_name'] 
            if taskStat not in tStat:
                print("%ERROR: new task status encountered: ",taskStat)
                tStat['unknown'] += 1
            else:
                tStat[taskStat] += 1
                pass
            tRows[row].insert(2, taskStat)
            pass

        ## Adjust titles (mostly to make them smaller)
        tTitles[tTitles.index('task_fail_count')] = '#fails'
        tTitles[tTitles.index('task_elapsed_time')] = 'duration\nhh:mm:ss'
        

        ## Pretty print task summary

        if opt == None:                 ## "Full" task summary
            print(tabulate(tRows,headers=tTitles,tablefmt=tblfmt))
            print('Task Status Summary: ',tStat)
            print('Log file directory: ',logDir)
        elif opt == "short":            ## "Short" task summary
            sSum = []
            for stat in tStat:
                sSum.append([stat,tStat[stat]])
            sSum.append(['total tasks',str(numTasks)])
            print(tabulate(sSum,['State','#'],tablefmt=tblfmt))
            pass

        return


    def fullSummary(self,runnum=None):
        ## This is the standard summary: workflow summary + summary of tasks in current run
        self.printWorkflowSummary(runnum)
        self.printTaskSummary(runnum)
        return


    def shortSummary(self,runnum=None):
        ## This is the short summary:
        self.printWorkflowSummary(runnum)
        self.printTaskSummary(runnum,opt='short')
        return


    def workflowHistory(self):
        ## This is the workflowHistory: details for each workflow 'run'
        sql = 'select workflow_name,user,host,time_began,time_completed,workflow_duration,tasks_completed_count,tasks_failed_count,rundir from workflow'
        (wrows,wtitles) = self.stdQuery(sql)
        ## Modify the result set
        for i in list(range(len(wtitles))):
            if wtitles[i] == 'tasks_completed_count':wtitles[i] = '#tasks_good'
            if wtitles[i] == 'tasks_failed_count':wtitles[i] = '#tasks_bad'
            pass
        rows = []
        wtitles.insert(0,"RunNum")
        for wrow in wrows:
            row = list(wrow)
            if row[4] is None: row[4] = '-> running or killed <-'
            row.insert(0,os.path.basename(row[8]))
            rows.append(row)
        ## Print the report
        print(tabulate(rows,headers=wtitles, tablefmt=tblfmt))
        return


####################################################
##
##                                   M A I N
##
####################################################


if __name__ == '__main__':


    reportTypes = ['fullSummary','shortSummary','workflowHistory']

    ## Parse command line arguments
    parser = argparse.ArgumentParser(description='A simple Parsl status reporter.  Available reports include:'+str(reportTypes))
    parser.add_argument('reportType',help='Type of report to display (default=%(default)s)',nargs='?',default='fullSummary')
    parser.add_argument('-f','--file',default='monitoring.db',help='name of Parsl monitoring database file (default=%(default)s)')
    parser.add_argument('-r','--runnum',type=int,help='Specific run number of interest (default = latest)')
    parser.add_argument('-s','--schemas',action='store_true',default=False,help="only print out monitoring db schema for all tables")
    parser.add_argument('-v','--version', action='version', version=__version__)
    args = parser.parse_args()

    print('\nwstat - Parsl workflow status (version ',__version__,', written for Parsl version '+pVersion+')\n')

    ## Create a Parsl Monitor object
    m = pmon(dbfile=args.file)

    ## Print out table schemas only
    if args.schemas:
        ## Fetch a list of all tables in this database
        tableList = m.getTableList()
        print('Tables: ',tableList)

        ## Print out schema for all tables
        for table in tableList:
            schema = m.getTableSchema(table)
            print(schema[0][0])
            pass
        sys.exit()

    ## Check validity of run number
    if not args.runnum == None and (int(args.runnum) > m.runmax or int(args.runnum) < m.runmin):
        print('%ERROR: Requested run number, ',args.runnum,' is out of range (',m.runmin,'-',m.runmax,')')
        sys.exit(1)
        
    ## Print out requested report
    if args.reportType not in reportTypes: sys.exit(1)

    if args.reportType == 'fullSummary':
        m.fullSummary(runnum=args.runnum)
    if args.reportType == 'shortSummary':
        m.shortSummary(runnum=args.runnum)
    if args.reportType == 'workflowHistory':
        m.workflowHistory()
 
    ## Done
    sys.exit()

