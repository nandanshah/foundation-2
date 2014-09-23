#!/bin/bash

#Checkf for sufficient arguments
if [ "$#" -lt 2 ]; then
    echo -e "Spark cluster URL or SPARK_HOME not set.\nUsage: exec.sh <spark-cluster-url> <spark-home>(optional)"
    exit 1
elif [ "$#" -eq 2 ]; then
    SPARK_CLUSTER=$1
    SPARK_HOME=$2
elif [ "$#" -eq 3 ]; then
    SPARK_CLUSTER=$1	
    SPARK_HOME=$2
    EXECUTOR_MEMORY=$3
fi

if [ -z $SPARK_CLUSTER ]; then
    echo -e "Spark cluster URL not set.\nUsage: exec.sh <spark-cluster-url> <spark-home>(optional)"
    exit 1
fi

if [ -z $SPARK_HOME ]; then
    echo -e "SPARK_HOME not set. Either set it as environment variable or pass as parameter to this script.\nUsage: exec.sh <spark-cluster-url> <spark-home>(optional)"
    exit 1
fi

if [ -z $EXECUTOR_MEMORY ]; then
    EXECUTOR_MEMORY=2GB
fi

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
echo -e "Submitting Spark Application, data-sync-consumer-1.0.0, to Spark Cluster $SPARK_CLUSTER"
$SPARK_HOME/bin/spark-submit --class com.dla.foundation.ESMovieSimilarity --master $SPARK_CLUSTER $SH_DIR/../lib/elasticsearch-movie-similarity-1.0.0.jar $SH_DIR/../conf/ES.properties $SH_DIR/../conf/Cassandra.properties $SH_DIR/../conf/Spark.properties --deploy-mode client --jars $JAR_LIST --executor-memory $EXECUTOR_MEMORY
