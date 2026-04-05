#!/bin/bash

echo "Create index using MapReduce pipelines"

INPUT_PATH="/input/data"
OUTPUT_PATH="/indexer/index"
hdfs dfs -rm -r -f $OUTPUT_PATH

mapred streaming \
  -files /app/mapreduce/mapper1.py,/app/mapreduce/reducer1.py \
  -input $INPUT_PATH \
  -output $OUTPUT_PATH \
  -mapper "python3 mapper1.py" \
  -reducer "python3 reducer1.py"

hdfs dfs -ls $OUTPUT_PATH
