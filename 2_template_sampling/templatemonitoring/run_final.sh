#!/bin/bash
export PYSPARK_PYTHON="/usr/local/python3.7.4/bin/python3"

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

ding_notice(){
      if [[ $# != 1 ]];then
          return 1
      fi
            echo "param is ~~~~~~~~~~~~"$1
      curl -X POST -H 'Content-Type: application/json' -d '{"accessToken": "36b4f1252f4482cbb592772f513e76bd3284c50bc5bfb534dbfc3a4d877d7da3","content": "'$1'" }' "http://10.10.10.50:8889/conch/notice/ding"
      return 0
    }

run_final(){
/usr/local/spark-2.4.3-bin-hadoop2.7/bin/spark-submit  \
    --jars mysql-connector-java-8.0.18.jar \
    --driver-memory 10g \
    --executor-memory 10g \
    --executor-cores 3 \
    --py-files hash.py,connector.py,utils.py,classifer.py,cluster.py,monitor.py,config.zip\
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
    --conf spark.dynamicAllocation.maxExecutors=50 \
    --conf spark.dynamicAllocation.executorIdleTimeout=100s \
    --conf spark.dynamicAllocation.cachedExecutorIdleTimeout=300s \
    --conf spark.scheduler.mode=FAIR \
    --conf spark.dynamicAllocation.schedulerBacklogTimeout=2s \
    --conf spark.default.parallelism=300 \
    --conf spark.sql.shuffle.partitions=300 \
    --conf spark.sql.broadcastTimeout=1800 \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  run_final.py $@
}

start=`date -d "$1" +%Y%m%d`
end=`date -d "$2" +%Y%m%d`
configfile=$3
#configfile="yaml/config_final.yaml"

while [[ "$start" -le "$end" ]]
do
the_date=`date -d "$end" +%Y-%m-%d`

# 生成增量数据
run_final --the_date $the_date --step 0 --config_file $configfile
if [[ $? != 0 ]]; then
    echo "${start} create incremt/tmp data  failed"
    exit 1
fi
notice="${the_date}全类别增量数据已聚类"
ding_notice "$notice" 

run_final --the_date $the_date --step 1 --config_file $configfile
if [[ $? != 0 ]]; then
    echo "${start} create incremt/tmp data  failed"
    exit 1
fi
notice="${the_date}全类别历史数据"
ding_notice "$notice" 

run_final --the_date $the_date --step 2 --config_file $configfile
if [[ $? != 0 ]]; then
    echo "${start} write base data failed"
    exit 1
fi
notice="${the_date}全类别模板合并成功"
ding_notice "$notice"  


let end=`date -d "+1 days ago $end" +%Y%m%d`
done

