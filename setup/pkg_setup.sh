inst_dir=$( cd $(dirname $BASH_SOURCE)/..; pwd -P)
export PYTHONPATH=${inst_dir}/python:${PYTHONPATH}
export PATH=${inst_dir}/bin:${PATH}
