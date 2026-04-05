#!/bin/bash

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python) 

unset PYSPARK_PYTHON

hdfs dfs -rm -r -f /input/data
hdfs dfs -rm -r -f /data
rm -rf /app/data
mkdir -p /app/data

hdfs dfs -put -f o.parquet / && \
    spark-submit prepare_data.py && \
    echo "Putting data to hdfs" && \
    hdfs dfs -put data / && \
    hdfs dfs -ls /data && \
    hdfs dfs -ls /input/data && \
    echo "done data preparation!"
