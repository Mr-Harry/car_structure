import pandas as pd
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.session import SparkSession
from string import Template
from datetime import datetime as dt
from datetime import timedelta
import sys
import subprocess
import os
import re
from pyspark import SparkConf, SparkContext
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Row
import argparse
from functools import partial
import pandas as pd
import logging
from libs.spark_base_connector import BaseSparkConnector

# 配置文件信息
os.environ['PYSPARK_PYTHON'] = '/usr/local/python3.7.4/bin/python3'

_sql = Template('''
select
row_key,
mobile_id,
event_time,
app_name,
suspected_app_name,
msg,
main_call_no,
abnormal_label,
hashcode,
'-2' as class_rule_id,
'normal' as ner_label,
1.0 as class_label_prob,
the_date,
file_no,
'${class_label}' as class_label 
from tmp_table
''')


class DataUploader(BaseSparkConnector):

    def __init__(self, app_name, log_level=None):
        """初始化
        初始化spark
        """
        # 初始化spark
        super().__init__(app_name=app_name, log_level=log_level)
        self.logger.info('分类上传模块初始化完毕')

    def run(self, domain_table, classified_table, the_date, file_no,single_partition=False):
        f_r = False if single_partition else True
        f_o = False if single_partition else True
        orig_data = self.read_partition(
            source_table=domain_table, the_date=the_date, file_no=file_no, sql_class='None',f_r=f_r,f_o=f_o)

        orig_data.createOrReplaceTempView('tmp_table')
        sql_string = _sql.substitute(class_label='None')
        new_data = self.spark.sql(sql_string)
        self.save_table(new_data, classified_table, 'class_label',single_partition=single_partition)


if __name__ == "__main__":
    # 定义参数
    parser = argparse.ArgumentParser(description="分类成果上传模块")
    parser.add_argument('--domain_table', default=None,
                        dest='domain_table', type=str, help='目标数据表')
    parser.add_argument('--classified_table', default=None,
                        dest='classified_table', type=str, help='分类成果表')
    parser.add_argument('--the_date', default=None,
                        dest='the_date', type=str, help='需要处理的the_date分区')
    parser.add_argument('--file_no', default=None,
                        dest='file_no', type=str, help='需要处理的file_no分区')
    parser.add_argument('--class_name', default=None,
                        dest='class_name', type=str, help='类目名称')
    parser.add_argument('--single_partition', default='False',type=str,help='输出表是否是单个file_no分区')
    args = parser.parse_args()
    single_partition = True if args.single_partition == 'True' else False
    # 初始化
    data_uploader = DataUploader(
        app_name=args.class_name+'_ClassDataUploader')
    # 运行
    data_uploader.run(domain_table=args.domain_table,
                      classified_table=args.classified_table,
                      the_date=args.the_date, file_no=args.file_no,single_partition=single_partition)
    # 结束
    data_uploader.stop()
