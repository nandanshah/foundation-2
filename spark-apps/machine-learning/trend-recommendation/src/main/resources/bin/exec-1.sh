#!/bin/bash

#Checkf for sufficient arguments
if [ "$#" -lt 1 ]; then
    echo -e "Spark cluster URL not set.\nUsage: exec.sh <spark-cluster-url> <spark-home>(optional)"
    exit 1
elif [ "$#" -eq 2 ]; then
    SPARK_HOME=$2
fi

if [ -z $SPARK_HOME ]; then
    echo -e "SPARK_HOME not set. Either set it as environment variable or pass as parameter to this script.\nUsage: exec.sh <spark-cluster-url> <spark-home>(optional)"
    exit 1
fi

EXECUTOR_MEMORY=$3

if [ -z $EXECUTOR_MEMORY ]; then
    EXECUTOR_MEMORY=2GB
fi

SPARK_CLUSTER=$1

#Get script directory
SH_DIR="$( cd "$( dirname "$0" )" && pwd )"

cd $SH_DIR/../lib/

#Prepare comma separated JAR files list from lib dir.
for jar in `ls`
do
JAR_LIST=$JAR_LIST,$SH_DIR/../lib/$jar
done
#Remove first comma (,)
JAR_LIST=`echo $JAR_LIST | sed -r 's/^.{1}//'`

#Submit application to Spark cluster
echo -e "Submitting Spark Application, trend-recommendation-UserEventSummaryDriver, to Spark Cluster $SPARK_CLUSTER"
$SPARK_HOME/bin/spark-submit --master $SPARK_CLUSTER --class com.dla.foundation.trendReco.UserEventSummaryDriver $SH_DIR/../lib/trend-recommendation-1.0.0.jar $SH_DIR/../conf/common.properties --jars $JAR_LIST --executor-memory $EXECUTOR_MEMORY

echo -e "Submitting Spark Application, trend-recommendation-DayScoreDriver, to Spark Cluster $SPARK_CLUSTER"
$SPARK_HOME/bin/spark-submit --master $SPARK_CLUSTER --class com.dla.foundation.trendReco.DayScoreDriver $SH_DIR/../lib/trend-recommendation-1.0.0.jar $SH_DIR/../conf/common.properties --jars $JAR_LIST --executor-memory $EXECUTOR_MEMORY
