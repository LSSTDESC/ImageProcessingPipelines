## wstat.py - workflow status summary from Parsl monitoring database

## The idea is not to replace the "sqlite3" interactive command or the
## Parsl web interface, but to complement them to create some useful
## interactive summaries specific to Parsl workflows.

## Python dependencies: sqlite3, tabulate

## T.Glanzman - Spring 2019
__version__ = "0.9.0 alpha"
pVersion='0.9.0'    ## Parsl version

import sys,os
import sqlite3
from tabulate import tabulate
import datetime
import argparse
import matplotlib.pyplot as plt
import pandas as pd



## Table format is used by 'tabulate' to select the text-based output format
## 'grid' looks nice but is non-compact
## 'psql' looks almost as nice and is more compact
tblfmt = 'psql'

class pmon:
    ### class pmon - interpret Parsl monitoring database
    def __init__(self,dbfile='monitoring.db',debug=0):
        ## Instance variables
        self.dbfile = dbfile
        self.debug = debug # [0=none,1=short,2=more,3=even more,5=lengthy tables]

        ## sqlite3 database connection and cursor
        self.con = sqlite3.connect(self.dbfile,
                                   detect_types=sqlite3.PARSE_DECLTYPES |
                                   sqlite3.PARSE_COLNAMES) ## special connect to sqlite3 file
#        self.con = sqlite3.connect(self.dbfile)           ## vanilla connect to sqlite3 file
        self.con.row_factory = sqlite3.Row                 ## optimize output format
        self.cur = self.con.cursor()                       ## create a 'cursor'

        ## List of all task stati defined by Parsl
        self.statList = ['pending','launched','runnable','running','retry','unsched','unknown','exec_done','memo_done','failed','dep_fail']

        ## Read in the workflow (summary) table
        self.wrows = None
        self.wtitles = None
        self.runid2num = None
        self.runnum2id = None
        self.numRuns = 0
        self.runmin = 999999999
        self.runmax = -1
        self.readWorkflowTable()

        ## pTasks contains selected task information filled by deepTaskSummary()
        self.pTitles = {'task_id':0,'task_name':1,'run_num':2,'status':3,'hostname':4,
                   '#fails':5,'submitTime':6,'startTime':7,'endTime':8,'runTime':9,'stdout':10}
        self.pTasks = []
        self.pTasksFilled = False
        self.taskLimit=0   # Set to non-zero to limit tasks processed for pTasks

        ## nodeUsage is a list of nodes currently in use and the
        ## number of tasks running on them.  {nodeID:#runningTasks}
        self.nodeUsage = {}
        
        ## This template contains all known parsl task states
        self.statTemplate = {'pending':0,'launched':0,'runnable':0,'running':0,'retry':0,'unsched':0,'unknown':0,'exec_done':0,'memo_done':0,'failed':0,'dep_fail':0}

        return


    def __del__(self):
        ## Class destructor 
        self.con.close()

    def stripms(self,intime):
        ## Trivial function to strip off millisec from Parsl time string
        if intime == None: return None
        return datetime.datetime.strptime(str(intime),'%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%d %H:%M:%S')

    def makedt(self,intime):
        ## Trivial function to return a python datetime object from Parsl time string
        return datetime.datetime.strptime(str(intime),'%Y-%m-%d %H:%M:%S.%f')
    

    def readWorkflowTable(self,sql="select * from workflow"):
        ## Extract all rows from 'workflow' table in monitoring.db
        if self.debug > 0:print("Entering readWorkflowTable, sql = ",sql)
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
        if self.debug > 1:
            print('runid2num = ',self.runid2num)
            print('runnum2id = ',self.runnum2id)
            print('runmin = ',self.runmin)
            print('runmax = ',self.runmax)
            pass
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



    def dumpTable(self,titles,rowz):
        ## Pretty print all rows with column names
        print("\ndumpTable:\n==========")
        print("titles = ",titles)
        print("Table contains ",len(rowz)," rows")
        for row in rowz:
            for key in row.keys():
                print(row[key])
            pass
        print("-------------end of dump--------------")
        return
    

    def stdQuery(self,sql):
        if self.debug > 0: print("Entering stdQuery, sql = ",sql)
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
        if self.debug > 0:
            print("titles = ",titles)
            print("#rows = ",len(rows))
            if self.debug > 4: print("rows = ",rows)
            pass
        return (rows,titles)


    def selectRunID(self,runnum=None):
        ## Select the workflow table row based on the requested
        ## runNumber (not to be confused with the many digit, hex
        ## "run_id")
        if self.debug>0: print("Entering selectRunID, runnum=",runnum)
        
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


    def timeDiff(self,start,end):
        ## Calculate difference in two (str) times from Parsl monitoringDB
        if start == None or end == None: return None
        #print('start = ',start,' (',type(start),'), end = ',end,' (',type(end),')')
        startDT = datetime.datetime.strptime(str(start),'%Y-%m-%d %H:%M:%S.%f')
        endDT = datetime.datetime.strptime(str(end),'%Y-%m-%d %H:%M:%S.%f')
        #print('startDT = ',startDT,' (',type(startDT),'), endDT = ',endDT,' (',type(endDT),')')
        diff = endDT-startDT
        #print('diff = ',diff,'   (',type(diff),')')
        return diff


    def getTaskStatus(self,runID,taskID):
        ## Return the status (task_status_name) for specified run and task
        if self.debug > 0 : print("Entering getTaskStatus(runID=",runID,",taskID=",taskID,")")
        ## Find the *status* for each task in this initial set
        ##   Obtain most recent status for the specified run and task
        sql = 'select task_id,timestamp,task_status_name from status where run_id="'+str(runID)+'" and task_id="'+str(taskID)+'" order by timestamp desc limit 1'
        (sRowz,sTitles) = self.stdQuery(sql)
        if self.debug > 1: self.dumpTable(sTitles,sRowz)
        taskStat = sRowz[0]['task_status_name'] 
        if taskStat not in self.statList:
            print("%ERROR: new task status encountered: ",taskStat)
            pass
        if self.debug > 0 : print("Returning task status = ",taskStat)
        return taskStat


    def getTaskRunData(self,runID,taskID,hashsum):
        ## Dig into the status and task tables for the actual entry that contains run information
        ## Signature: (xRunID,xHost,xSubmitTime,xStartTime,xEndTime,xStatus) = self.getTaskRunData(runID,taskID,hashsum)
        if self.debug > 0: print("Entering getTaskRunData(runID=",runID,",taskID=",taskID,",hashsum=",hashsum)
        #
        # Query task table for all tasks with taskID and hashsum
        sql = 'select run_id,task_id,task_func_name,hostname,task_fail_count,task_time_submitted,task_time_running,task_time_returned,task_elapsed_time,task_stdout,task_hashsum from task where task_id="'+str(taskID)+'" and task_hashsum="'+str(hashsum)+'" order by run_id asc'
        (tRowz,tTitles) = self.stdQuery(sql)
        if self.debug > 1: self.dumpTable(tTitles,tRowz)

        # Query status table for candidate runs
        sql = 'select run_id,task_id,timestamp,task_status_name from status where task_status_name="exec_done" and task_id="'+str(taskID)+'" order by timestamp desc'
        (sRowz,sTitles) = self.stdQuery(sql)
        if self.debug > 4: self.dumpTable(sTitles,sRowz)

        if len(sRowz) > 1:
            print("Trouble in getTaskRunData: multiple exec_done entries for this run/task")
            sys.exit(4)
            pass
        
        for tRow in tRowz:
            if tRow['run_id'] == sRowz[0]['run_id']:
                if self.debug > 1: print("We have a match! run_id = ",tRow['run_id'])
                if self.debug > 0 : print("Returning")
                return (tRow['run_id'],tRow['hostname'],tRow['task_time_submitted'],tRow['task_time_running'],tRow['task_time_returned'],sRowz[0]['task_status_name'])
            pass
        if self.debug > 0 : print("Returning")
        return (None,None,None,None,None,None)

        

    def deepTaskSummary(self,runnum=None,opt=None,dig=True,printSummary=True):
        ## The task summary is a composite collection of values from
        ## the 'task' and 'status' tables

        ## TO-DO: the "runnum" parameter is not properly being used
        
        ## deepTaskSummary() differs from printTaskSummary in that
        ## individual tasks are tracked, as necessary (and if
        ## requested), to the run in which they successfully ran.
        ## Also, printing the taskSummary is optional but the data is
        ## preserved for plotting

        ## deepTaskSummary() is responsible for filling:
        ##   self.pTasks[] list with selected task info
        ##   self.nodeUsage{} with node usage statistics
        
        ##  Select requested initial Run in workflow table
        rowindex = self.selectRunID(runnum)
        wrow = self.wrows[rowindex]
        if runnum == None: runnum = int(self.runid2num[wrow['run_id']])  # most recent run
        if self.debug > 0: print('--> run_id = ',wrow['run_id'])

        ##  Query the 'task' table for initial workflow run
        runID = wrow['run_id']
        sql = 'select task_id,task_func_name,hostname,task_fail_count,task_time_submitted,task_time_running,task_time_returned,task_elapsed_time,task_stdout,task_hashsum from task where run_id = "'+wrow['run_id']+'" order by task_id asc'
        (tRowz,tTitles) = self.stdQuery(sql)
        if self.debug > 1: print('tTitles = ',tTitles)


        ##  Prepare master list of tasks (order is important: must match self.pTitles, above)
        ##   and keep an eye out for tasks completed in previous run
        rCount = 0
        nRunning = 0
        for row in tRowz:
            ## Default task summary (with a few dummies to be filled in later)
            rCount += 1
            pTask = [row['task_id'],
                     row['task_func_name'],
                     runnum,
                     'dummy',
                     row['hostname'],
                     row['task_fail_count'],
                     self.stripms(row['task_time_submitted']),
                     self.stripms(row['task_time_running']),
                     self.stripms(row['task_time_returned']),
                     self.timeDiff(row['task_time_running'],row['task_time_returned']),
                     os.path.splitext(row['task_stdout'])[0]]
                                  
            taskStat = self.getTaskStatus(runID,row['task_id'])
            pTask[self.pTitles['status']] = taskStat

            ## Fill nodeUsage dict
            
            if taskStat == "running":
                nRunning += 1
                if row['hostname'] not in self.nodeUsage:
                    self.nodeUsage[row['hostname']] = 1
                else:
                    self.nodeUsage[row['hostname']] += 1
                    pass
                pass

            ## If task was executed in a previous run, fetch runtime data
            if taskStat == "memo_done" and dig:
                if self.debug > 1: print("Dig deeper...")
                (xRunID,xHost,xSubmitTime,xStartTime,xEndTime,xStatus) = self.getTaskRunData(runID,row['task_id'],row['task_hashsum'])
                pTask[self.pTitles['run_num']]    = int(self.runid2num[xRunID])
                #pTask[self.pTitles['status']]     = xStatus
                pTask[self.pTitles['hostname']]   = xHost
                pTask[self.pTitles['submitTime']] = self.stripms(xSubmitTime)
                pTask[self.pTitles['startTime']]  = self.stripms(xStartTime)
                pTask[self.pTitles['endTime']]    = self.stripms(xEndTime)
                pTask[self.pTitles['runTime']]    = self.timeDiff(xStartTime,xEndTime)
                      

            ## Final step for this task is adding it to the full task summary list
            self.pTasks.append(pTask)
            
            ## (Possibly) limit # of tasks processed -- most a development/debugging feature
            if self.taskLimit > 0 and rCount > self.taskLimit: break
            
            pass
        
        ## Print out full task table
        if printSummary: print(tabulate(self.pTasks,headers=list(self.pTitles.keys()),tablefmt=tblfmt))
        self.pTasksFilled = True

        ## Print out node usage summary
        if len(self.nodeUsage) > 0:
            print('\nNumber of running tasks = ',nRunning)
            print('Number of active nodes = ',len(self.nodeUsage))
            for node in self.nodeUsage:
                print(node, self.nodeUsage[node])
                pass
            pass
        

        return

    #########################################################################################
    #########################################################################################
    #########################################################################################
    #########################################################################################
    #########################################################################################

    def printStatusMatrix(self,runnum=None):
        ## Confirm pTasks[] has been filled
        if not self.pTasksFilled: self.deepTaskSummary(runnum=runnum,printSummary=False)

        ## if no tasks defined, bail
        if len(self.pTasks) < 1:
            print("Nothing to do: pTasks is empty!")
            return
        else:
            #print("There are ",len(self.pTasks)," tasks in pTasks.")
            pass


        ## Tally status for each task type
        ##  Output to taskStats{}:
        ##     taskStats{'taskname1':{#status1:num1,#status2:num2,...},...}
        ##  Then convert into a Pandas dataframe
        statList = self.statTemplate.keys()   # list of all status names
        taskStats = {}   # {'taskname':{statTemplate}}
        tNameIndx = self.pTitles['task_name']
        tStatIndx = self.pTitles['status']
        tRunIndx  = self.pTitles['runTime']
        nTaskTypes = 0
        nTasks = 0
        for task in self.pTasks:
            nTasks += 1
            tName = task[tNameIndx]
            tStat = task[tStatIndx]
            if tName not in taskStats.keys():
                nTaskTypes += 1
                taskStats[tName] = dict(self.statTemplate)
                pass
            taskStats[tName][tStat] += 1
            pass

        ## df = pandas.DataFrame(columns=['a','b','c','d'], index=['x','y','z'])
        ## df.loc['y'] = pandas.Series({'a':1, 'b':5, 'c':2, 'd':3})
        pTaskStats = pd.DataFrame(columns=self.statTemplate.keys(), index=taskStats.keys())
        for task in taskStats:
            pTaskStats.loc[task] = pd.Series(taskStats[task])
            pass

        print('\n\tTASK STATUS MATRIX: \n\n',pTaskStats,'\n\n')
        return




    
    def plotStats(self,runnum=None):
        ## plot statistics for most current parsl run
        #
        
        ## Confirm pTasks[] has been filled
        if not self.pTasksFilled: self.deepTaskSummary(runnum=runnum,printSummary=False)

        ## if no tasks defined, bail
        if len(self.pTasks) < 1:
            print("Nothing to do: pTasks is empty!")
            return
        else:
            print("There are ",len(self.pTasks)," tasks in pTasks.")
            pass
        
        #
        ## Sort task list by task type
        ##  Input from pTasks[]:
        ##     pTitles = {'task_id':0,'task_name':1,'run_num':2,'status':3,'hostname':4,
        ##     '#fails':5,'submitTime':6,'startTime':7,'endTime':8,'runTime':9,'stdout':10}

        ## Tally execution runtimes for "done" tasks
        histData = {}
        tNameIndx = self.pTitles['task_name']
        tStatIndx = self.pTitles['status']
        tRunIndx  = self.pTitles['runTime']
        nTasks = 0
        nTaskTypes = 0
        nDone = 0
        print('tNameIndx = ',tNameIndx)
        for task in self.pTasks:
            nTasks += 1
            tName = task[tNameIndx]
            tStat = task[tStatIndx]
            if tStat.endswith('done'):
                nDone += 1
                if task[tRunIndx] == None:
                    print('%ERROR: monitoring.db bug.  Completed task has no runtime: ',task[:9])
                    continue
                if task[tNameIndx] not in histData.keys():
                    nTaskTypes += 1
                    histData[tName] = [task[tRunIndx].total_seconds()/60]
                else:
                    histData[tName].append(task[tRunIndx].total_seconds()/60)
                    pass
                pass
            pass

        print('Total tasks = ',nTasks,'\nTotal task types = ',nTaskTypes,'\nTotal tasks done = ',nDone)
        if self.debug > 1 :
            for task in histData:
                print('task: ',task,', len = ',len(histData[task]))
                pass
            pass

        #
        ## Histogram run time separately for each task type
        #print('task = ',task,', histData[task] = ',str(histData[task]))
        #a, b, c = plt.hist(histData[task],bins=100,histtype='step')  # <-- this DOES work!

        ## The following stanza was borrowed from ancient RSP code
        # fig = plt.figure(figsize=(11,8.5))  ## Establish canvas
        # plt.suptitle("Task Runtimes)   ## define plot title (before making plots)
        # ax = fig.add_subplot(411)  ## 411 => 4 rows x 1 col of plots, this is plot #1
        # #ax.plot(timex,flux,'k-',timex,flux,'r,',label="FLUX",linewidth=0.5,drawstyle='steps-mid')
        # ax.set_ylabel('# tasks')
        # ax.set_xlabel('Runtime (min)')
        # ax.grid(True)
        # #ax.ticklabel_format(style='sci',scilimits=(0,0),axis='y')  ## Numerical formatting
        # plt.setp(ax.get_xticklabels(), visible=False)

        ## matplotlib's hist function:
        # matplotlib.pyplot.hist(x, bins=None, range=None,
        # density=None, weights=None, cumulative=False, bottom=None,
        # histtype='bar', align='mid', orientation='vertical',
        # rwidth=None, log=False, color=None, label=None,
        # stacked=False, normed=None, *, data=None, **kwargs)[source]

        ## Setup histograms
        nHists = len(histData.keys())
        print("Preparing ",nHists," histograms.")
        ncols = 3 # number of histograms across the page
        if nHists%ncols == 0:
            nrows = int(nHists/ncols)
        else:
            nrows = int(nHists/ncols) + 1
            pass
        print("(ncols,nrows) = (",ncols,nrows,")")
        num_bins = 50


        ## matplotlib.pyplot.subplots(nrows=1, ncols=1, sharex=False, sharey=False, squeeze=True, subplot_kw=None, gridspec_kw=None, **fig_kw)[source]
        fig, ax = plt.subplots(nrows=nrows,ncols=ncols)

        for task in histData.keys():

            print(task)

            # the histogram of the data
            n, bins, patches = ax.hist(histData[task], num_bins)

            # add a 'best fit' line
            # y = ((1 / (np.sqrt(2 * np.pi) * sigma)) *
            #      np.exp(-0.5 * (1 / sigma * (bins - mu))**2))
            #ax.plot(bins, y, '--')
            ax.set_xlabel('Execution time on Cori KNL (min)')
            ax.set_ylabel('# Tasks')
            ax.grid(True)
            ax.set_title('Distribution of '+task+' execution times')

            # Tweak spacing to prevent clipping of ylabel
            fig.tight_layout()
            plt.show()
        
            pass

        #
        ## Display histograms
        #
        plt.show()
        return

    
    #########################################################################################
    #########################################################################################
    #########################################################################################
    #########################################################################################
    #########################################################################################

    def printTaskSummary(self,runnum=None,opt=None):
        ##############################################################
        ## (NOTE: as of 4/20/2020 this function is mostly obsolete,
        ## use deepTaskSummary() instead)
        ##############################################################
        
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

            ## Calculate run duration (wall clock time while running)
            startTime = tRows[-1][startTimeIndx]
            endTime = tRows[-1][endTimeIndx]
            tRows[-1][elapsedIndx] = self.timeDiff(startTime,endTime)

            for ix in [subTimeIndx,startTimeIndx,endTimeIndx]:
                if tRows[-1][ix] != None:
                    tRows[-1][ix] = self.stripms(tRows[-1][ix])
                    pass
                pass
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
        tStat = dict(self.statTemplate)
        for row in range(numTasks):
            taskID = tRows[row][0]
            #print('runID = ',runID,', taskID = ',taskID)
            sql = 'select task_id,timestamp,task_status_name from status where run_id="'+str(runID)+'" and task_id="'+str(taskID)+'" order by timestamp desc limit 1'
            (sRowz,sTitles) = self.stdQuery(sql)
            if self.debug > 4: self.dumpTable(sTitles,sRowz)
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


    def newSummary(self,runnum=None,dig=True,printSummary=True):
        ## This is the new standard summary: workflow summary + summary of tasks in current run
        self.printWorkflowSummary(runnum)
        self.deepTaskSummary(dig=dig,printSummary=printSummary)
        self.printStatusMatrix(runnum=runnum)
        return


    def fullSummary(self,runnum=None):
        ## This is the standard summary: workflow summary + summary of tasks in current run
        self.printWorkflowSummary(runnum)
        self.printTaskSummary(runnum)
        return


    def shortSummary(self,runnum=None):
        ## This is the short summary:
        self.printWorkflowSummary(runnum)
        self.printStatusMatrix(runnum=runnum)
        self.printTaskSummary(runnum,opt='short')
        return

    def plot(self,runnum=None):
        self.printWorkflowSummary()
        #self.deepTaskSummary(runnum=runnum,dig=True,printSummary=False)
        self.printStatusMatrix(runnum=runnum)
        self.plotStats(runnum=runnum)
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


#############################################################################
#############################################################################
##
##                                   M A I N
##
#############################################################################
#############################################################################


if __name__ == '__main__':


    reportTypes = ['fullSummary','shortSummary','runHistory','newSummary','plot']

    ## Parse command line arguments
    parser = argparse.ArgumentParser(description='A simple Parsl status reporter.  Available reports include:'+str(reportTypes))
    parser.add_argument('reportType',help='Type of report to display (default=%(default)s)',nargs='?',default='newSummary')
    parser.add_argument('-f','--file',default='monitoring.db',help='name of Parsl monitoring database file (default=%(default)s)')
    parser.add_argument('-r','--runnum',type=int,help='Specific run number of interest (default = latest)')
    parser.add_argument('-s','--schemas',action='store_true',default=False,help="only print out monitoring db schema for all tables")
    parser.add_argument('-t','--taskXdata',action='store_false',default=True,help="disable digging into past runs for full task execution data")
    parser.add_argument('-d','--debug',type=int,default=0,help='Set debug level (default = %(default)s)')


    parser.add_argument('-v','--version', action='version', version=__version__)


    args = parser.parse_args()

    print('\nwstat - Parsl workflow status (version ',__version__,', written for Parsl version '+pVersion+')\n')

    startTime = datetime.datetime.now()

    ## Create a Parsl Monitor object
    m = pmon(dbfile=args.file)
    m.debug = args.debug

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
    if args.reportType == 'fullSummary':
        m.fullSummary(runnum=args.runnum)
    elif args.reportType == 'shortSummary':
        m.shortSummary(runnum=args.runnum)
    elif args.reportType == 'runHistory':
        m.workflowHistory()
    elif args.reportType == 'newSummary':
        #m.taskLimit=100
        m.newSummary(runnum=args.runnum,dig=args.taskXdata,printSummary=True)
    elif args.reportType == 'plot':
        m.plot()
    else:
        print("%ERROR: Unrecognized reportType: ",args.reportType)
        print("Must be one of: ",reportTypes)
        print("Exiting...")
        sys.exit(1)
        pass
    
    ## Done
    endTime = datetime.datetime.now()
    print("wstat elapsed time = ",endTime-startTime)
    sys.exit()

