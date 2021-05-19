#!/bin/bash

export PATH=$1

source activate master_thesis 2> /dev/null
if [ $? -ne 0 ]
then
    (>&2 echo '### [ERR]: conda environment not sourced correctly (scheduler)')
fi

ulimit -c 0

nohup dask-scheduler &> "$2/scheduler-${PBS_JOBID}.log" &
