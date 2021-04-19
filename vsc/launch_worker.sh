#!/bin/bash -l

export PATH=$1

source activate master_thesis 2> /dev/null
if [ $? -ne 0 ]
then
    (>&2 echo '### [ERR]: conda environment not sourced correctly (worker)')
fi

nohup dask-worker $3 &> "$2/worker-$(hostname)-${PBS_JOBID}.log" &
