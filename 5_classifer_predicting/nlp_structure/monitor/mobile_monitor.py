import argparse
import json
import logging
import os
import re
import subprocess
import copy
import sys
from datetime import datetime as dt
from datetime import timedelta
from functools import partial
from string import Template

import pandas as pd

# from libs.clean_udf import *
from libs.spark_base_connector import BaseSparkConnector
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql import functions
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window
from libs.utils import import_fun, import_module, DataTool

ABSPATH = os.path.dirname(os.path.abspath(__file__))
# 配置文件信息
os.environ['PYSPARK_PYTHON'] = '/usr/local/python3.7.4/bin/python3'


host="10.10.10.50"
port="3306"
user="sf"
passwd="KiYQ0g&CJgo!cgWR"
database="nlp_structure_old"
log_table = "nlp_count_test" # 任务执行成功记录表

class DataMonitor(BaseSparkConnector):
    def __init__(self, app_name, log_level=None):
        """
        数据清洗模块
        Args:
            app_name: 必填参数，用于标记Spark任务名称;  str
            log_level: 选填参数，用于标记Spark任务的日志等级，只可以为WARN、INFO、DEBUG、ERROR其中一种;  str
        """
        # 初始化spark
        super().__init__(app_name=app_name, log_level=log_level)
        # 加载完毕
        self.logger.info('数据监控模块初始化完成')
    def run(self, nlp_table_dict,full_label_dict, col_config_dict,the_date, file_no,class_name,dt):
        count_data = self.read_partition_nlp_mobile_id_monitor(nlp_config=nlp_table_dict,the_date=the_date,file_no=file_no)
        count_data.cache()
        class_count = count_data.groupBy('count_item').agg({"mobile_id": "count"}).withColumnRenamed('count(mobile_id)','count')
        class_count.cache()
        all_count = count_data.select('mobile_id').dropDuplicates().count()
        all_count_df = self.spark.createDataFrame([{'count_item':class_name,'count':all_count}]).select('count_item','count')
        count = class_count.union(all_count_df).withColumn("the_date", functions.lit(the_date)).withColumn("class_name", functions.lit(class_name)).toJSON().collect()
        sqls = []
        for i in count:
            v = json.loads(i)
            _sql = _sql = "INSERT INTO nlp_structure_old.nlp_count_mobile_test(count_item,count,the_date,class_name)VALUES ('{0}', {1}, '{2}', '{3}') ON DUPLICATE KEY UPDATE count = {1};".format(v['count_item'],v['count'],v['the_date'],v['class_name'])
            sqls.append(_sql)
        if len(sqls) > 0:
            res = dt.run_sql(sqls)
            if res == False:
                raise Exception("任务失败！")
            print(res)

if __name__ == "__main__":
    # 定义参数
    parser = argparse.ArgumentParser(description="数据清洗模块")
    parser.add_argument('--config', default=None,
                        dest='config', type=str, help='配置参数信息')
    parser.add_argument('--col_config', default=None,
                        dest='col_config', type=str, help='ner&dwb配置参数信息')
    parser.add_argument('--the_date', default=None,
                        dest='the_date', type=str, help='需要处理的the_date分区')
    parser.add_argument('--file_no', default=None,
                        dest='file_no', type=str, help='需要处理的file_no分区')
    parser.add_argument('--class_name', default=None,
                        dest='class_name', type=str, help='类目名称')
    parser.add_argument('--config_name', default=None,
                        dest='config_name', type=str, help='配置文件夹名称')
    parser.add_argument('--dict_list_file', default=None,
                        dest='dict_list_file', type=str, help='需要加载的字典列表')

    args = parser.parse_args()
    # 解析配置信息
    config_dict = json.load(open(args.config, 'r', encoding='utf-8'))
    col_config_dict = json.load(open(args.col_config, 'r', encoding='utf-8'))
    the_date = args.the_date
    file_no = args.file_no
    nlp_table_dict = config_dict.get('nlp_table_dict', {})
    dwb_table_dict = config_dict.get('dwb_table_dict', {})
    full_label_dict = config_dict.get('full_label_dict', {})
    dict_list = json.load(open(args.dict_list_file, 'r', encoding='utf-8')
                          ) if args.dict_list_file is not None else []
    # 初始化
    data_monitor = DataMonitor(app_name=args.class_name+'_data_monitor')
    clean_udf_dict = import_fun(
        'config.' + args.config_name+'.clean_udf.clean_udf_dict')
    dt = DataTool(host=host,port=port,username=user,password=passwd,database=database)
    data_monitor.run(nlp_table_dict=nlp_table_dict,
                    full_label_dict=full_label_dict,
                    col_config_dict=col_config_dict,
                    the_date=args.the_date,
                    file_no=args.file_no,
                    class_name=args.class_name, dt=dt)
    # 结束
    data_monitor.stop()
