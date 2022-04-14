#!/bin/bash
config_name=$1
domain_sample_hash=$2'_sample_hash'
domain_merge_hash=$2'_sample_merge'

echo $config_name $domain_sample_hash

beeline -u "jdbc:hive2://coprocessor01-fcy.hadoop.dztech.com:2181,coprocessor02-fcy.hadoop.dztech.com:2181,coprocessor03-fcy.hadoop.dztech.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "
create table if not exists nlp_dev.$domain_sample_hash
(
    row_key String COMMENT '唯一编码',
    mobile_id String COMMENT 'id',
    app_name String COMMENT '清洗签名',
    suspected_app_name String COMMENT '原始签名',
    msg String COMMENT '文本内容',
    hashcode String COMMENT 'msg在finial表的编码'
)COMMENT '$config_name' partitioned BY(
    the_month string COMMENT '业务日期yyyy-MM格式')
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
STORED AS orc;"
if [[ $? != 0 ]]; then
  echo "nlp_dev.${domain_sample_hash}表已存在"
  exit 1
fi

beeline -u "jdbc:hive2://coprocessor01-fcy.hadoop.dztech.com:2181,coprocessor02-fcy.hadoop.dztech.com:2181,coprocessor03-fcy.hadoop.dztech.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "
create table if not exists nlp_dev.$domain_merge_hash
(
    row_key String COMMENT '唯一编码',
    mobile_id String COMMENT 'id',
    app_name String COMMENT '清洗签名',
    suspected_app_name String COMMENT '原始签名',
    msg String COMMENT '文本内容',
    hashcode String COMMENT 'msg在finial表的编码',
    new_hash String COMMENT 'msg的simhash编码',
    cnt bigint COMMENT 'new_hashcode计数值'
)COMMENT '$config_name' partitioned BY(
    the_month string COMMENT '业务日期yyyy-MM格式')  
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
STORED AS orc;
"
if [[ $? != 0 ]]; then
  echo "nlp_dev.${domain_merge_hash}表已存在"
  exit 1
fi
