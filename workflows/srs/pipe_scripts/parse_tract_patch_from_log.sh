#!/bin/bash


for logfile in /sps/lsst/users/descprod/Pipeline2/Logs/DC2DM_3_COADD/1.6/task_multiBandDriver/run_multiBandDriver/00*/*/logFile.txt; do 
    status=$(grep "Exit status" $logfile)
    if [[ "$status" == *"0"* ]]; then
        script=$(grep "CUR_SCRIPT" $logfile)
        IFS="=" read dummy script_name <<< $script
	if [ -f $script_name ]; then
            mbd=$(grep multiBandDriver $script_name)
            IFS="=" read dummy1 dummy2 dummy3 dummy4 <<< $mbd
            read tract dummy <<< $dummy2
            read patches dummy <<< $dummy3
            patches=${patches//^/ }
            read filters dummy <<< $dummy4
            filters=${filters//^/ }
            for filt in $filters;do
		for patch in $patches; do
                    echo /sps/lsst/dataproducts/desc/DC2/Run1.2p/w_2018_30/rerun/coadd-all2/deepCoadd_results/"$filt"_t"$tract"_p"$patch"
		done
            done
	fi
    fi
done

