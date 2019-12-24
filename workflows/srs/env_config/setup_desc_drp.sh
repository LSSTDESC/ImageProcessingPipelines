#!/bin/sh

if [ -z "$1" ]
then	
	echo "Please provide a full path install directory"
	exit 1
fi
mkdir -p $1
export PATH=$1/bin:$PATH

export STACKCVMFS=/cvmfs/sw.lsst.eu/linux-x86_64/lsst_distrib
export DESC_STACK_VER=v19.0.0

echo "${DESC_STACK_VER}" > $1/stack_version

if [[ $SITE == "NERSC" ]]; then
    module unload python
    module swap PrgEnv-intel PrgEnv-gnu
    module swap gcc gcc/8.2.0
    module rm craype-network-aries
    module rm cray-libsci
    module unload craype
    export CC=gcc
#else
	# CC environment setup for CVMFS

fi


# Version List
export DESC_GCR_VER=0.8.8
export DESC_GCRCatalogs_VER=v0.14.3
export DESC_ngmix_VER=1.3.4
export DESC_ngmix_VER_STR=v$DESC_ngmix_VER
export DESC_meas_extensions_ngmix_VER=0.9.5
export DESC_DC2_PRODUCTION_VER=0.4.0
export DESC_DC2_PRODUCTION_VER_STR=v$DESC_DC2_PRODUCTION_VER
export DESC_OBS_LSST_VER=19.0.0-run2.2-v1
export DESC_SIMS_CI_PIPE_VER=0.1.1

source $STACKCVMFS/$DESC_STACK_VER/loadLSST.bash
setup lsst_distrib

# pip install what we can, using a constraints file and local root directory
pip install -c ./dm-constraints-py3-4.6.8.txt --prefix $1 GCR==$DESC_GCR_VER
pip install -c ./dm-constraints-py3-4.6.8.txt --prefix $1 https://github.com/LSSTDESC/gcr-catalogs/archive/$DESC_GCRCatalogs_VER.tar.gz

export PYTHONPATH=$PYTHONPATH:$1/lib/python3.7/site-packages 
curdir=$PWD
cd $1

#DC2-production
curl -LO https://github.com/LSSTDESC/DC2-production/archive/$DESC_DC2_PRODUCTION_VER_STR.tar.gz
tar xvfz $DESC_DC2_PRODUCTION_VER_STR.tar.gz
rm $DESC_DC2_PRODUCTION_VER_STR.tar.gz
ln -s DC2-production-$DESC_DC2_PRODUCTION_VER DC2-production

# Install ngmix, requires numba which is already included in DM env
curl -LO https://github.com/esheldon/ngmix/archive/$DESC_ngmix_VER_STR.tar.gz
tar xzf $DESC_ngmix_VER_STR.tar.gz
cd ngmix-$DESC_ngmix_VER
python setup.py install --prefix=$1
cd ..
rm $DESC_ngmix_VER_STR.tar.gz
ln -s ngmix-$DESC_ngmix_VER ngmix

# Install meas_extensions_ngmix
curl -LO https://github.com/lsst-dm/meas_extensions_ngmix/archive/v$DESC_meas_extensions_ngmix_VER.tar.gz
tar xzf v$DESC_meas_extensions_ngmix_VER.tar.gz
cd meas_extensions_ngmix-$DESC_meas_extensions_ngmix_VER
setup -r . -j
scons
cd ..
rm v$DESC_meas_extensions_ngmix_VER.tar.gz
ln -s meas_extensions_ngmix-$DESC_meas_extensions_ngmix_VER meas_extensions_ngmix

# install obs_lsst
curl -LO https://github.com/lsst/obs_lsst/archive/$DESC_OBS_LSST_VER.tar.gz
tar xvfz $DESC_OBS_LSST_VER.tar.gz 
ln -s obs_lsst-$DESC_OBS_LSST_VER obs_lsst
cd obs_lsst
setup -r . -j
scons
cd ..             
rm $DESC_OBS_LSST_VER.tar.gz

# install sims_ci_pipe
curl -LO https://github.com/LSSTDESC/sims_ci_pipe/archive/$DESC_SIMS_CI_PIPE_VER.tar.gz
tar xvzf $DESC_SIMS_CI_PIPE_VER.tar.gz
ln -s sims_ci_pipe-$DESC_SIMS_CI_PIPE_VER sims_ci_pipe
cd sims_ci_pipe
setup -r . -j    
scons
cd ..

cd $curdir
echo
echo "Installation Complete"
echo
echo "When setting up this DMstack env, append "$1"/lib/python3.7/site-packages to the PYTHONPATH"

