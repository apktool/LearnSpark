#!/usr/bin/env bash

bin=$(dirname "${BASH_SOURCE-$0}")
bin=$(cd "$bin" > /dev/null || exit; pwd)
cd "$bin" || exit

cd ..

spark-submit --class com.spark.WordCountDemo \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 4g \
    --executor-memory 2g \
    --executor-cores 1 \
    --queue default \
    LearnSpark-1.0-SNAPSHOT.jar
