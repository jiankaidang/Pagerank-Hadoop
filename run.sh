#!/bin/sh

#There are 3 Hadoop jobs to compute the pagerank

N=10 #termination condition

myflow_id=$1
input_s3=$2
output_directory=$3

#Use AWS CLI to copy jar files to input_s3 (S3)
aws s3 cp hadoop-pagerank.jar $input_s3/

#RUN THE FIRST JOB
elastic-mapreduce -j $myflow_id --jar $input_s3/hadoop-pagerank.jar --arg Job1 --arg s3://cs9223/pagelinks-en-all.csv
elastic-mapreduce -j $myflow_id --wait-for-steps

#RUN THE SECOND JOB
for ((i=1; i<=$N; i++))
do
        #Make sure that the output of ith iteration is the input of (i+1)th iteration
        elastic-mapreduce -j $myflow_id --jar $input_s3/hadoop-pagerank.jar --arg Job2 --arg $i
        elastic-mapreduce -j $myflow_id --wait-for-steps
done

#RUN THE THIRD JOB
#Output of the last iteration of job2 will be the input of job3
elastic-mapreduce -j $myflow_id --jar $input_s3/hadoop-pagerank.jar --arg Job3 --arg $output_directory
elastic-mapreduce -j $myflow_id --wait-for-steps

aws s3 cp $output_directory/top100/part-r-00000 $output_directory/top100.txt