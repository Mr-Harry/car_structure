#!/bin/bash
export PYSPARK_PYTHON="/usr/local/python3.7.4/bin/python3"

/usr/local/spark-2.4.3-bin-hadoop2.7/bin/spark-submit  \
    --jars mysql-connector-java-8.0.18.jar \
    --driver-memory 10g \
    --executor-memory 10g \
    --executor-cores 3 \
    --py-files hash.py,connector.py,utils.py,cluster.py\
    --packages graphframes:graphframes:0.8.1-spark2.4-s_2.11 \
    --conf spark.yarn.executor.memoryOverhead=6g \
    --conf spark.driver.memoryOverhead=6g \
    --conf spark.sql.autoBroadcastJoinThreshold=500485760 \
    --conf spark.network.timeout=800000 \
    --conf spark.driver.maxResultSize=4g \
    --conf spark.rpc.message.maxSize=500 \
    --conf spark.rpc.askTimeout=600 \
    --conf spark.executor.heartbeatInterval=60000 \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.shuffle.service.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=1 \
    --conf spark.dynamicAllocation.maxExecutors=50 \
    --conf spark.dynamicAllocation.executorIdleTimeout=100s \
    --conf spark.dynamicAllocation.cachedExecutorIdleTimeout=300s \
    --conf spark.scheduler.mode=FAIR \
    --conf spark.dynamicAllocation.schedulerBacklogTimeout=2s \
    --conf spark.default.parallelism=300 \
    --conf spark.sql.shuffle.partitions=300 \
    --conf spark.sql.broadcastTimeout=1800 \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  main.py $@
