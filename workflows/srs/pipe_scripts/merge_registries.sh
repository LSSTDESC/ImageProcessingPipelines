#!/bin/bash

final_db=$3
db_list=$2
echo "merging $db_list into $final_db"
for db in $db_list; do
    if [ -f $final_db ]; then
	echo "merging $db into $final_db"
	if [ "$1" == "TRACT2VISIT" ]; then
	    db_visit=`sqlite3 $db "select distinct visit from overlaps;"`
	    check_visit=`sqlite3 $final_db "select distinct visit from overlaps where visit=$db_visit;"`
	    if [ "$check_visit" == "$db_visit" ]; then
		echo "$db_visit is already present in $final_db... shying away from merging!"
		continue;
	    fi
	    count=`sqlite3 ${final_db} "select count(*) from overlaps;"`
	    offset=`sqlite3 ${db} "select count(*) from overlaps;"`
	elif [ "$1" == "RAW2VISIT" ]; then
	    count=`sqlite3 ${final_db} "select count(*) from raw;"`
	    offset=`sqlite3 ${db} "select count(*) from raw;"`
	    db_visit=`sqlite3 $db "select distinct visit from raw_visit;"`
	fi
	echo "adding $offset to $count rows"
	if [ "$1" == "TRACT2VISIT" ]; then
	    sqlite3 $final_db "attach '$db' as toMerge;insert into overlaps select id+$count,tract,patch,visit,detector,filter,layer from toMerge.overlaps;"
	elif [ "$1" == "RAW2VISIT" ]; then 
	    sqlite3 $final_db "attach '$db' as toMerge;insert into raw select id+$count,run,visit,filter,date,dateObs,expTime,raftName,detectorName,detector,snap,object,imageType,testType,lsstSerial,wavelength from toMerge.raw;"
	    for visit in $db_visit; do
		check_visit=`sqlite3 $final_db "select distinct visit from raw_visit where visit=$visit;"`
		if [[ -z $check_visit ]]; then
		    sqlite3 $final_db "attach '$db' as toMerge;insert into raw_visit select visit,filter,dateObs,expTime from toMerge.raw_visit where visit=$visit;"
		fi
	    done
	fi
    else
	echo "cp $db $final_db"
	cp $db $final_db
    fi
done
