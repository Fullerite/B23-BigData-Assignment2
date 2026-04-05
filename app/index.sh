#!/bin/bash

echo "Creating indexes"
./create_index.sh

echo "Storing indexes to Cassandra"
./store_index.sh

echo "Indexing pipeline finished"
hdfs dfs -ls /
