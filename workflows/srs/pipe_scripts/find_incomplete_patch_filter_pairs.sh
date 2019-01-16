#!/bin/bash

logfiles="/sps/lsst/users/descprod/Pipeline2/Logs/DC2DM_3_COADD/1.5/task_multiBandDriver/run_multiBandDriver/*/*//logFile.txt"
#logfiles="./logFile.txt"


for logfile in $logfiles; do
    #echo $logfile
    while read -r start ; do
        IFS="'" read dummy1 dummy tract dummy dummy filter dummy dummy dummy patch dummy <<< $start
        tract=$(echo $tract | tr -d ":, ")
        read process dummy <<< $dummy1
        while read -r stop ; do
            IFS="'" read dummy dummy tract2 dummy dummy filter2 dummy dummy dummy patch2 dummy <<< $stop
            tract2=$(echo $tract2 | tr -d ":, ")
            if [ "$tract" = "$tract2" -a "$patch" = "$patch2" -a "$filter" = "$filter2" ]; then
                COMPLETED=True
                break
            fi
        done < <(grep "Finished measurement" $logfile | grep $process)
        if [[ -z $COMPLETED ]]; then
            echo $process $tract $patch $filter INCOMPLETE
	    mv rerun/coadd-all2/deepCoadd-results/merged/$tract/$patch/ref-* salvaged/.
	    mv rerun/coadd-all2/deepCoadd-results/$filter/$tract/$patch/meas-* salvaged/.
	    mv rerun/coadd-all2/deepCoadd_results/${filter}_t${tract}_p${patch}/forced-* salvaged/.
        else
            #echo $process $tract $patch $filter COMPLETED;
            unset COMPLETED
        fi
    done < <(grep "Start measurement" $logfile)
done



for logfile in $logfiles; do
    #echo $logfile
    while read -r start ; do
        IFS="'" read dummy1 dummy tract dummy dummy filter dummy dummy dummy patch dummy <<< $start
        tract=$(echo $tract | tr -d ":, ")
        read process dummy <<< $dummy1
        while read -r stop ; do
            IFS="'" read dummy dummy tract2 dummy dummy filter2 dummy dummy dummy patch2 dummy <<< $stop
            tract2=$(echo $tract2 | tr -d ":, ")
            if [ "$tract" = "$tract2" -a "$patch" = "$patch2" -a "$filter" = "$filter2" ]; then
                COMPLETED=True
                break
            fi
        done < <(grep "Finished forced photometry" $logfile | grep $process)
        if [[ -z $COMPLETED ]]; then
            echo $process $tract $patch $filter INCOMPLETE
	    mv rerun/coadd-all2/deepCoadd_results/${filter}_t${tract}_p${patch}/forced-* salvaged
        else
            #echo $process $tract $patch $filter COMPLETED;
            unset COMPLETED
        fi
    done < <(grep "Start forced photometry" $logfile)
done

