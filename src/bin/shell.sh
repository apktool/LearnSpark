#!/usr/bin/env bash

bin=$(dirname "${BASH_SOURCE-$0}")
bin=$(cd "$bin" > /dev/null || exit; pwd)
cd "$bin" || exit

cd ..
libs=$(echo lib/*)
jars=${libs// /,}

spark-submit --class com.hbase.rdd.HBaseSparkRDDReadDemo \
    --jars $jars \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 4g \
    --executor-memory 2g \
    --executor-cores 1 \
    --queue default \
    LearnSpark-1.0-SNAPSHOT.jar
