# 抽样数据流
本数据流的功能：
1. 按照大表的表结构建新表
2. 修改新表的数据存储格式为textfile
3. 将大表的某个分区的前10条数据写入新表

在本数据流中
大表是`preprocess.ds_txt_final_sample`
小表是`nlp_dev.qianyu_20220318`
最终的小表的数据，可以通过如下命令，以txt格式在本地路径(服务器)生成
```bash
hdfs dfs -cat 'hdfs://fcycdh/user/hive/warehouse/nlp_dev.db/qianyu_20220318/the_date=2021-12-02/file_no=merge_20211202_1202_L0/part-00000-61596164-a9ba-4e59-a62c-192d8f69e9a8.c000'>log.out
# 将黑框输出的内容重定向到日志文件log.out
```

## 0_create_table.sh
```bash
#!/bin/bash
source /etc/profile
source ~/.bash_profile

source_database_name=$1
source_table_name=$2
new_database_name=$3
new_table_name=$4

sql_part="
drop table if exists ${new_database_name}.${new_table_name};
create table ${new_database_name}.${new_table_name} like ${source_database_name}.${source_table_name};
alter table ${new_database_name}.${new_table_name} set fileformat textfile;
alter table ${new_database_name}.${new_table_name} set serdeproperties('serialization.format'='\t', 'field.delim'='\t');
"

beeline -u "jdbc:hive2://coprocessor01-fcy.hadoop.dztech.com:2181,coprocessor02-fcy.hadoop.dztech.com:2181,coprocessor03-fcy.hadoop.dztech.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "$sql_part"
if [[ $? != 0 ]];then
echo "sql 运行失败！！！！！！"
exit 1
fi
echo 建表完成
```
脚本逻辑
1. 建表sql的代码逻辑
- 修改文件存储类型(set fileformat)
- 修改列分隔符(set serdeproperties)
- 旧分区要drop掉

2. beeline的命令行的脚本逻辑
- -u参数，连接hive
- -e参数，sql代码

3. shell脚本的if判断逻辑
- 如果代码运行失败，输出："sql 运行失败！！！！！！"
- 如果代码运行成功，输出：建表完成

## 1_insert_data.sh
```bash
#!/bin/bash
source /etc/profile
source ~/.bash_profile

rss_path=$1
source_database_name=$2
source_table_name=$3
new_database_name=$4
new_table_name=$5
pt=$6

sql_part="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${new_database_name}.${new_table_name} partition(the_date,file_no) select * from ${source_database_name}.${source_table_name} where the_date='${pt}' limit 1000;
"

cd /home/${rss_path}/ && bash rss.sh "data_explorer" "nlp_dev" "$sql_part"

if [[ $? != 0 ]];then
echo "sql 运行失败！！！！！！"
exit 1
fi
echo 数据写入完成
```
脚本逻辑
1. `set hive.exec.dynamic.partition.mode=nonstrict;`这个属性默认值是strict,就是要求分区字段必须有一个是静态的分区值，当前设置为nonstrict,那么可以全部动态分区
2. 向动态分区插入数据
3. 切换到`/home/nlp_dev`路径，运行`rss.sh`脚本，使用spark_sql跑插入语句的sql代码
4. shell脚本的if判断逻辑
- 如果代码运行失败，输出："sql 运行失败！！！！！！"
- 如果代码运行成功，输出：建表完成

## run.sh
```bash
#!/bin/bash -e
baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)
bash ${baseDirForScriptSelf}/0_create_table.sh preprocess ds_txt_final_sample nlp_dev qianyu_20220318
bash ${baseDirForScriptSelf}/1_insert_data.sh nlp_dev preprocess ds_txt_final_sample nlp_dev qianyu_20220318 2021-11
```
脚本逻辑
1. 变量`baseDirForScriptSelf`是当前脚本所在的路径
2. 串联了0_create_table.sh和1_insert_data.sh两个脚本的运行

# 配置脚本
## beeline.sh
```bash
beeline -u "jdbc:hive2://coprocessor01-fcy.hadoop.dztech.com:2181,coprocessor02-fcy.hadoop.dztech.com:2181,coprocessor03-fcy.hadoop.dztech.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2"
```

## rss.sh
```bash
#!/bin/bash
. /etc/profile
. ~/.bash_profile

#执行spark-sql
# 参数1：任务名称
# 参数2：执行队列名
# 参数3：执行的sql

if [[ $# != 3 ]];then
    echo "`date '+%Y-%m-%d %H:%M:%S'` | [ERROR] |  run_spark_sql error, error args num"
    exit 1
fi

if [[ "$3" = "" ]];then
    echo "`date '+%Y-%m-%d %H:%M:%S'` | [ERROR] | run_spark_sql error, the sql string can not be empty"
    exit 1
fi


run_spark_l(){
    echo -e "start execute sql:\n$3"
    /usr/local/spark-2.4.3-bin-hadoop2.7/bin/spark-sql --driver-memory 4g \
    --executor-memory 6g \
    --executor-cores 2 \
    --conf spark.yarn.executor.memoryOverhead=4g \
    --conf spark.driver.memoryOverhead=1g \
    --conf spark.sql.autoBroadcastJionThreshold=500485760 \
    --conf spark.network.timeout=800000 \
    --conf spark.driver.maxResultSize=4g \
    --conf spark.rpc.message.maxSize=500 \
    --conf spark.rpc.askTimeout=600 \
    --conf spark.executor.heartbeatInterval=60000 \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.shuffle.service.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=5 \
    --conf spark.dynamicAllocation.maxExecutors=200 \
    --conf spark.dynamicAllocation.executorIdleTimeout=100s \
    --conf spark.dynamicAllocation.cachedExecutorIdleTimeout=300s \
    --conf spark.scheduler.mode=FAIR \
    --conf spark.dynamicAllocation.schedulerBacklogTimeout=2s \
    --conf spark.default.parallelism=400 \
    --conf spark.sql.shuffle.partitions=400 \
    --conf spark.sql.broadcastTimeout=1800 \
    --conf spark.maxRemoteBlockSizeFetchToMem=512m \
    --name "$1" \
    --queue "$2" \
    -e "$3"
    if [[  $? == 0 ]];then
        echo -e "`date '+%Y-%m-%d %H:%M:%S'` | [INFO] | spark-sql execute success,sql:\n$3"
        exit 0
    else
        exit 1
    fi
}

run_spark_l  "$1" "$2" "$3"
```

