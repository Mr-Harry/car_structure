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
insert overwrite table ${new_database_name}.${new_table_name} partition(the_date,file_no) select * from ${source_database_name}.${source_table_name} where the_date='${pt}' limit 10000;
"

cd /home/${rss_path}/ && bash rss.sh "data_explorer" "nlp_dev" "$sql_part"

if [[ $? != 0 ]];then
echo "sql 运行失败！！！！！！"
exit 1
fi
echo 数据写入完成