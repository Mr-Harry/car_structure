#!/bin/bash
export PYSPARK_PYTHON="/usr/local/python3.7.4/bin/python3"
baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)
parentDirForScriptSelf=$(cd "$(dirname "$0")"; cd ".."; pwd)

rm config_2.zip
zip -r config_2.zip config_sample -x "./config_sample/.git/*"
md5_1=`md5sum config_2.zip`
md5_0=`md5sum config_sample.zip`
md5_1=`echo $md5_1|cut -c1-33`
md5_0=`echo $md5_0|cut -c1-33`
if [ "$md5_0" = "$md5_1" ];then
    echo "config_sample文件夹无更新"
else
    echo "config_sample文件夹更新！！！"
    mv config_2.zip config_sample.zip
fi

app_name=$1
config_name=$2
start_month=$3
end_month=$4
sample_ratio=$5

bash ${baseDirForScriptSelf}/create_sql.sh $app_name $config_name
if [[ $? != 0 ]]; then
  echo "表已存在"
  exit 1
fi


/usr/local/spark-2.4.3-bin-hadoop2.7/bin/spark-submit  \
    --jars mysql-connector-java-8.0.18.jar \
    --driver-memory 10g \
    --executor-memory 10g \
    --executor-cores 3 \
    --packages graphframes:graphframes:0.8.1-spark2.4-s_2.11 \
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
    --conf spark.default.parallelism=1000 \
    --conf spark.sql.shuffle.partitions=1000 \
    --conf spark.sql.broadcastTimeout=1800 \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--py-files ${parentDirForScriptSelf}/config_sample.zip,${parentDirForScriptSelf}/utils.py,${baseDirForScriptSelf}/tool.py,${parentDirForScriptSelf}/monitor.py,${parentDirForScriptSelf}/cluster.py,${parentDirForScriptSelf}/hash.py,${parentDirForScriptSelf}/connector.py,${parentDirForScriptSelf}/classifer.py \
${baseDirForScriptSelf}/sample_merge.py $config_name $start_month $end_month $sample_ratio