#!/bin/bash -l

export PATH=$1

source activate master_thesis 2> /dev/null
if [ $? -ne 0 ]
then
    (>&2 echo '### [ERR]: conda environment not sourced correctly (worker)')
fi

ulimit -c 0

nohup dask-worker $3 --memory-limit 15GB --nprocs 12 --nthreads 1 --interface ib0 --local-directory $4 &> "$2/worker-$(hostname)-${PBS_JOBID}.log" &

