#!/bin/sh

if [ -z "$1" ]
then	
	echo "Please provide a full path install directory"
	exit 1
fi

export STACKCVMFS=/cvmfs/sw.lsst.eu/linux-x86_64/lsst_distrib
export DESC_STACK_VER=$2

echo "${DESC_STACK_VER}" > $1/stack_version

if [[ $SITE == "NERSC" ]]; then
    module unload python
    module swap PrgEnv-intel PrgEnv-gnu
    module swap gcc gcc/7.3.0
    module rm craype-network-aries
    module rm cray-libsci
    module unload craype
    export CC=gcc
#else
	# CC environment setup for CVMFS

fi


# Version List
#export DESC_pyarrow_VER=0.13.0
export DESC_GCR_VER=0.8.8
export DESC_GCRCatalogs_VER=v0.14.3
export DESC_ngmix_VER=1.3.4
export DESC_ngmix_VER_STR=v$DESC_ngmix_VER
export DESC_meas_extensions_ngmix_VER=0.9.5
export DESC_obs_lsst_VER=18.1.0-run2.2-v1

source $STACKCVMFS/$DESC_STACK_VER/loadLSST.bash

# pip install what we can, using a constraints file and local root directory
#pip install -c ./dm-constraints-py3-4.5.12.txt --prefix $1 pyarrow==$DESC_pyarrow_VER
pip install -c ./dm-constraints-py3-4.5.12.txt --prefix $1 GCR==$DESC_GCR_VER
pip install -c ./dm-constraints-py3-4.5.12.txt --prefix $1 https://github.com/LSSTDESC/gcr-catalogs/archive/$DESC_GCRCatalogs_VER.tar.gz

curdir=$PWD
cd $1

#install obs_lsst
git clone git@github.com:lsst/obs_lsst.git
cd obs_lsst
git checkout $DESC_obs_lsst_VER
setup -r . -j 
scons
eups declare -r . obs_lsst dc2-run2.2
cd ..

#install DC2-production
git clone git@github.com:LSSTDESC/DC2-production.git

# Install ngmix, requires numba which is already included in DM env
curl -LO https://github.com/esheldon/ngmix/archive/$DESC_ngmix_VER_STR.tar.gz
tar xzf $DESC_ngmix_VER_STR.tar.gz
cd ngmix-$DESC_ngmix_VER
python setup.py install --prefix=$1
cd ..
rm $DESC_ngmix_VER_STR.tar.gz
ln -s ngmix-$DESC_ngmix_VER ngmix

export PYTHONPATH=$PYTHONPATH:$1/lib/python3.7/site-packages 

# Install meas_extensions_ngmix
setup lsst_distrib
curl -LO https://github.com/lsst-dm/meas_extensions_ngmix/archive/$DESC_meas_extensions_ngmix_VER.tar.gz
tar xzf $DESC_meas_extensions_ngmix_VER.tar.gz
cd meas_extensions_ngmix-$DESC_meas_extensions_ngmix_VER
setup -r . -j
scons
cd ..
rm $DESC_meas_extensions_ngmix_VER.tar.gz
ln -s meas_extensions_ngmix-$DESC_meas_extensions_ngmix_VER meas_extensions_ngmix
cd $curdir

echo
echo "Installation Complete"
echo
echo "When setting up this DMstack env, append "$1"/lib/python3.7/site-packages to the PYTHONPATH"

