#!/bin/bash

final_db=$3
db_list=$2
echo "merging $db_list into $final_db"
for db in $db_list; do
    if [ -f $final_db ]; then
	echo "merging $db into $final_db"
	if [ "$1" == "TRACT2VISIT" ]; then
	    db_visit=`sqlite3 $db "select distinct visit from overlaps;"`
	    Check_visit=`sqlite3 $final_db "select distinct visit from overlaps where visit=$db_visit;"`
	    if [ "$check_visit" == "$db_visit" ]; then
		echo "$db_visit is already present in $final_db... shying away from merging!"
		continue;
	    fi
	    count=`sqlite3 ${final_db} "select count(*) from overlaps;"`
	    offset=`sqlite3 ${db} "select count(*) from overlaps;"`
	    count2=`sqlite3 ${final_db} "select count(*) from conditions;"`
	    offset2=`sqlite3 ${db} "select count(*) from conditions;"`
	elif [ "$1" == "RAW2VISIT" ]; then
	    count=`sqlite3 ${final_db} "select count(*) from raw;"`
	    offset=`sqlite3 ${db} "select count(*) from raw;"`
	    db_visit=`sqlite3 $db "select distinct visit from raw_visit;"`
	fi
	echo "adding $offset to $count rows"
	if [ "$1" == "TRACT2VISIT" ]; then
	    sqlite3 $final_db "attach '$db' as toMerge;insert into overlaps select id+$count,tract,patch,visit,detector,filter,layer from toMerge.overlaps;"
	    sqlite3 $final_db "attach '$db' as toMerge;insert into conditions select id+$count2,visit,detector,filter,mjd,airmass,psf_ixx,psf_iyy,psf_ixy,psf_detradius,psf_traradius,psf_detfhwm,psf_trafhwm,a_pxsq,a_arsecsq,mag5sigma,ccd_corner_1_ra,ccd_corner_1_dec,ccd_corner_2_ra,ccd_corner_2_dec,ccd_corner_3_ra,ccd_corner_3_dec,ccd_corner_4_ra,ccd_corner_4_dec,mean_variance,median_variance,std_variance,mean_sig,median_sig,std_sig,zeroflux,trsf_zflux,zeroflux_njy,trsf_zflux_njy,calib_mean,calib_err,twenty_flux,twentytwo_flux from toMerge.conditions;"
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
