#!/bin/bash
export PYSPARK_PYTHON="/usr/local/python3.7.4/bin/python3"

start="2021-03-29"
end="2021-08-01"
config_name=$3
config_zh=`grep class_label $config_name | awk -F 'class_label: ' '{print $2}' `
# 判断开始日期是否是周一，不是的话一直前进到最近的周一
flag=`date -d "$start" +%a`
while [[ $flag != "Mon" ]]
do
let start=`date -d "-1 days ago $start" +%Y%m%d`
flag=`date -d "$start" +%a`
done

ding_notice(){
      if [[ $# != 1 ]];then
          return 1
      fi
            echo "param is ~~~~~~~~~~~~"$1
      curl -X POST -H 'Content-Type: application/json' -d '{"accessToken": "36b4f1252f4482cbb592772f513e76bd3284c50bc5bfb534dbfc3a4d877d7da3","content": "'$1'" }' "http://10.10.10.50:8889/conch/notice/ding"
      return 0
    }

function run_industry(){
/usr/local/spark-2.4.3-bin-hadoop2.7/bin/spark-submit  \
    --jars mysql-connector-java-8.0.18.jar \
    --driver-memory 10g \
    --executor-memory 10g \
    --executor-cores 3 \
    --py-files hash.py,connector.py,utils.py,classifer.py,cluster.py,monitor.py\
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
  run_industry_add.py $@
}


while [[ "$start" -lt "$end" ]]
do
the_date=`date -d "$start" +%Y-%m-%d`
end_date=`date -d "-6 days ago $start" +%Y-%m-%d` 
echo "${the_date}至${end_date}处理中"
run_industry --start "$the_date" --end "$end_date" --step 0 --config_file $config_name
if [[ $? != 0 ]]; then
    echo "${the_date}至${end_date} create incremt data  failed"
    exit 1
fi
notice="${the_date}至${end_date}${config_zh}--当天模板已聚类"
ding_notice "$notice"


python3 action_classifier/action_classifier.py --config_file $config_name
if [[ $? != 0 ]]; then
    echo "${the_date}至${end_date} create incremt data  failed"
    exit 1
fi
notice="${the_date}至${end_date}${config_zh}--当天模板行为已分类"
ding_notice "$notice"


run_industry --start "$the_date" --end "$end_date" --step 1 --config_file $config_name
if [[ $? != 0 ]]; then
    echo "${the_date}至${end_date} write tmp data failed"
    exit 1
fi
notice="${the_date}至${end_date}${config_zh}--当天增量模板已处理"
ding_notice "$notice"


run_industry --start "$the_date" --end "$end_date" --step 2 --config_file $config_name
if [[ $? != 0 ]]; then
    echo "${the_date}至${end_date} write base data failed"
    exit 1
fi
notice="${the_date}至${end_date}${config_zh}模板已合并"
ding_notice "$notice"

echo "${the_date}至${end_date}${config_zh}模板已合并" >>start_bank.log
let start=`date -d "-7 days ago $start" +%Y%m%d`
done