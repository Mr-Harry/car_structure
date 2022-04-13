#!/bin/bash
# 按 T-13 的时间间隔跑数
cd /home/nlp/nearduplicateremoval
start=`date -d "20210329" +%Y%m%d`

donedatefile="tools/done.log"
doneweekfile="tools/week.log"
done_date=`grep done_date $donedatefile |awk -F 'done_date=' '{print $2}'`
today=`date  +%Y%m%d`
end=`date -d "+13 days ago $today" +%Y%m%d`

find_date(){
  for d2 in $done_date
  do
    if [[ $1 == ${d2} ]]; then
      return 1
    fi
  done
  return 0
}

hdfs dfs -ls /user/hive/warehouse/nlp_dev.db/templet_monitoring_base_table_bank |grep week_ >$doneweekfile
done_week=`grep week_ $doneweekfile |awk -F 'week_' '{print $2}'`
find_week(){
  for d2 in $done_week
  do
    if [[ $1 == ${d2} ]]; then
      return 1
    fi
  done
  return 0
}

while [[ "$start" -le "$end" ]]
do

  # 判断该日期是否跑过，没跑的话进行跑数
  find_date $start
  if [[ $? == 0 ]]; then
    sh run_final.sh $start $start yamls/config_final.yaml
    if [[ $? == 0 ]]; then
        echo "done_date=${start}" >>$donedatefile
    else
        exit 1
    fi
  fi
'''
  #判断是否是周日，若是则生成这周的行业数据
  flag=`date -d "$start" +%a`
  if [[ $flag == "Sun" ]]; then
    industry_start=`date -d "+6 days ago $start" +%Y%m%d`
    industry_end=`date -d "$start" +%Y%m%d`
    find_week "${industry_start}_${industry_end}"
    if [[ $? == 0 ]]; then
      sh run_industry.sh $industry_start $industry_end yamls/config_industry_bank.yaml
      if [[ $? == 0 ]]; then
        echo "行业跑数${industry_start}_${industry_end}完成"
      else
        exit 1
      fi
      
    fi
  fi
'''
  let start=`date -d "-1 days ago $start" +%Y%m%d`
done

sh data_BI/DataBI.sh --config_file data_BI/bi_config/final.yaml --task_type final