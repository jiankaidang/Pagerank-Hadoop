#!/bin/sh
#There are 3 Hadoop jobs to compute the pagerank
N=10 #termination condition
input_directory=$1
input_filename=$2
output_directory=$3
#RUN THE FIRST JOB
hadoop jar hadoop-streaming.jar $input_directory/$input_filename