#!/bin/bash
source ../../../env.sh
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 --conf spark.default.parallelism=2 ./violationDate.py parking_violations.csv
