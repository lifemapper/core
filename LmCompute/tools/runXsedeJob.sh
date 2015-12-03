#!/bin/bash

export PATH=${PATH}:${HOME}/software/bin
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${HOME}/software/lib

MF_FILE="charlie.makeflow"
MASTER_PROC_ID="0"
MF_PROJECT="charlieMF"

if [ "$SLURM_PROCID" = "$MASTER_PROC_ID" ]; then
   # Start up the makeflow master process
   echo "This is the master process"
   makeflow -T wq -N $MF_PROJECT -d all $MF_FILE
else
   # Start up a worker
   echo "This is a worker (Process $SLURM_PROCID)"
   work_queue_worker -N $MF_PROJECT -d all -a
fi
