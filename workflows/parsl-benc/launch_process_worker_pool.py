#!/bin/bash

shifter --image=lsstdesc/stack-sims:w_2019_19-sims_w_2019_19 bash -c "$(pwd)/launch_process_worker_pool_inner $(pwd) $*"

