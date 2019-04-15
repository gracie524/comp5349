#!/bin/bash
mkdir ~/data
aws s3 cp s3://aws-logs-335478698019-us-east-1/AllVideos_short.csv ~/assignment

spark-submit \
    --master local[4] \
    workload1.py \
    --input file:///home/hadoop/assignment/ \
    --output file:///home/hadoop/result/
