#/bin/bash

## initRepo.sh - establish a empty DM-style repository
## 

## NOTE: you must have a DM environment set up prior to running this
## script, as well as particulars concerning how to initialize the
## repo, i.e., the $PT_ variables.  This is typically done by sourcing
## configTask.sh followed by cvmfsSetup.sh


echo `date`"  Entering initRepo.sh"

PWDSAVE=$PWD

##### Initialize output repo (but only once!)
###if [ ! -d ${PT_REPODIR} ]; then
    mkdir ${PT_REPODIR}
    cd ${PT_REPODIR}

    echo "lsst.obs.lsst.imsim.ImsimMapper" > ${PT_REPODIR}/_mapper

    # echo `date` " [Setup CALIB]"
    tar -xvzf $PT_CALIBS
    echo "[rc = "$?"]"
    # cd CALIB                     ## 10/31/2019 No longer necessary
    # python symlink_flats.py      ## Link all filters to i-band
    # echo "[rc = "$?"]"
    # cd ..

    ## Copy in Reference Catalogs
    echo `date` " [Setup Reference Catalogs]"
    tar -xvf $PT_REFCAT

    ## Copy in the (fake) brighter-fatter gains
    echo `date` " [Setup BF gains]"
    cp -pr $PT_BFKERNELS ${PT_REPODIR}

    # ## Ingest some simulated data
    # echo `date` " [ingestDriver.py image data]"
    # # Note that "--cores" can probably be '1' as we're only creating sym-links
    # ingestDriver.py ${PT_REPODIR} @$PWDSAVE/${PT_INGEST} --cores 5 --mode link --output ${PT_REPODIR} -c allowError=True register.ignore=True
    # echo "[rc = "$?"]"

    cd $PWDSAVE    # return to original directory
# else
#     echo "%ALERT: the requested repo directory already exists.  No action taken"
#     echo ${PT_REPODIR}
# fi


echo `date`"  Exiting initRepo.sh"
 

exit 
