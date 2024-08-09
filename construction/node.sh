#!/bin/ksh

SRC=$1
DEST=$2
MASTER=$3

line=`head -$SLURM_ARRAY_TASK_ID $MASTER | tail -1`

pwd
../node.py $SRC $DEST $line
