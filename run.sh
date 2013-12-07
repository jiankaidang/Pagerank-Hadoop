#!/bin/sh

#There are 3 Hadoop jobs to compute the pagerank

N=10 #termination condition

input_directory=$1
input_filename=$2
output_directory=$3

#RUN THE FIRST JOB
hadoop jar hadoop-pagerank.jar Job1 $input_directory/$input_filename

#RUN THE SECOND JOB
for ((i=1; i<=$N; i++))
do
        #Make sure that the output of ith iteration is the input of (i+1)th iteration
        hadoop jar hadoop-pagerank.jar Job2 $i

        #Remove the output of previous iterations here. we only need output of the most recent iteration.
        rm -rf Job2/loop$(($i - 1))

        echo DONE ITERATION $i
done
