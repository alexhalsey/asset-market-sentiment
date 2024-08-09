#!/bin/ksh

SRC=$1
DEST=$2
MASTER=$3

cnt=`cat $MASTER | wc -l | cut -d' ' -f1`

echo $cnt

rm -Rf log_files
mkdir log_files

cd log_files
sbatch -a 1-$cnt ../node.sh $SRC $DEST ../$MASTER

