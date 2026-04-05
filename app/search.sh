#!/bin/bash

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python) 
unset PYSPARK_PYTHON

QUERY="$1"

spark-submit \
    --master yarn \
    --deploy-mode client \
    /app/query.py "$QUERY"
