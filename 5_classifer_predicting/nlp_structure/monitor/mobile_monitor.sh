#!/bin/bash
export PYSPARK_PYTHON="/usr/local/python3.7.4/bin/python3"
baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)
parentDirForScriptSelf=$(cd "$(dirname "$0")"; cd ".."; pwd)
grandParentDirForScriptSelf=$(cd "$(dirname "$0")"; cd "../.."; pwd)

cd ${parentDirForScriptSelf}
rm libs_1.zip
zip -r libs_1.zip libs
md5_1=`md5sum libs_1.zip`
md5_0=`md5sum libs.zip`
md5_1=`echo $md5_1|cut -c1-33`
md5_0=`echo $md5_0|cut -c1-33`
if [ "$md5_0" = "$md5_1" ];then
    echo "lib 文件夹无更新"
else
    echo "lib 文件夹更新！！！"
    mv libs_1.zip libs.zip
fi

rm config_1.zip
zip -r config_1.zip config -x "./config/.git/*"
md5_1=`md5sum config_1.zip`
md5_0=`md5sum config.zip`
md5_1=`echo $md5_1|cut -c1-33`
md5_0=`echo $md5_0|cut -c1-33`
if [ "$md5_0" = "$md5_1" ];then
    echo "config文件夹无更新"
else
    echo "config文件夹更新！！！"
    mv config_1.zip config.zip
fi

/usr/local/spark-2.4.3-bin-hadoop2.7/bin/spark-submit  \
    --driver-memory 3g \
    --executor-memory 3g \
    --executor-cores 3 \
    --py-files ${parentDirForScriptSelf}/libs.zip,${parentDirForScriptSelf}/config.zip \
    --conf spark.yarn.executor.memoryOverhead=6g \
    --conf spark.driver.memoryOverhead=6g \
    --conf spark.sql.autoBroadcastJionThreshold=500485760 \
    --conf spark.network.timeout=800000 \
    --conf spark.driver.maxResultSize=4g \
    --conf spark.rpc.message.maxSize=500 \
    --conf spark.rpc.askTimeout=600 \
    --conf spark.executor.heartbeatInterval=60000 \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.shuffle.service.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=1 \
    --conf spark.dynamicAllocation.maxExecutors=100 \
    --conf spark.dynamicAllocation.executorIdleTimeout=100s \
    --conf spark.dynamicAllocation.cachedExecutorIdleTimeout=300s \
    --conf spark.scheduler.mode=FAIR \
    --conf spark.dynamicAllocation.schedulerBacklogTimeout=2s \
    --conf spark.default.parallelism=400 \
    --conf spark.sql.shuffle.partitions=400 \
    --conf spark.sql.broadcastTimeout=1800 \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  ${baseDirForScriptSelf}/mobile_monitor.py $@


