#!/bin/bash -l

export PATH=$1

source activate master_thesis 2> /dev/null
if [ $? -ne 0 ]
then
    (>&2 echo '### [ERR]: conda environment not sourced correctly (worker)')
fi

ulimit -c 0

# Run workers as threads to share 'heavy' datasets in shared memory
nohup dask-worker $3 --memory-limit 20GB --nprocs 36 --nthreads 1 --interface ib0 --local-directory $4 &> "$2/worker-$(hostname)-${PBS_JOBID}.log" &
