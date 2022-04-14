import pandas as pd
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.session import SparkSession
import json
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
from libs.spark_base_connector import BaseSparkConnector, domain_schema
from libs.utils import import_object, import_module, import_fun
from functools import partial
# 配置文件信息
os.environ['PYSPARK_PYTHON'] = '/usr/local/python3.7.4/bin/python3'


def target_data_extractor(row, dict_list, domain_extractor):
    return domain_extractor(row.msg, row.app_name, row.suspected_app_name, row.hashcode, row.abnormal_label, dict_list)


# 目标数据抽取函数封装
def target_data_extractor_generator(dict_list, domain_extractor):
    return partial(target_data_extractor, dict_list=dict_list, domain_extractor=domain_extractor)


class DataExtractor(BaseSparkConnector):
    def __init__(self, app_name, log_level=None):
        """初始化
        初始化spark

        Args:
            app_name: 必填参数，用于标记Spark任务名称;  str
            log_level: 选填参数，用于标记Spark任务的日志等级，只可以为WARN、INFO、DEBUG、ERROR其中一种;  str
        """
        # 初始化spark
        super().__init__(app_name=app_name)

    def run(self, source_table, domain_table, the_date, file_no, dict_list, domain_extractor,single_partition=False):
        """ 执行目标数据获取任务

            Args:
                source_table: 必填参数，上游数据表;  str
                domain_table: 必填参数，目标数据表;  str
                the_date: 必填参数，待处理分区;  str
                file_no: 必填参数，待处理分区;  str
        """
        # 读取数据
        f_r = False if single_partition else True
        f_o = False if single_partition else True
        orig_data = self.read_partition(
            source_table=source_table, the_date=the_date, file_no=file_no, sql_class='domain',f_r=f_r,f_o=f_o)
        # 进行过滤
        target_data_extractor_func = target_data_extractor_generator(
            dict_list, domain_extractor)
        target_data = self.spark.createDataFrame(
            orig_data.rdd.filter(target_data_extractor_func), domain_schema)
        self.save_table(target_data, domain_table,single_partition=single_partition)


if __name__ == "__main__":
    # 定义参数
    parser = argparse.ArgumentParser(description="目标数据获取模块")
    parser.add_argument('--source_table', default='preprocess.ds_txt_final',
                        dest='source_table', type=str, help='上游数据表')
    parser.add_argument('--domain_table', default=None,
                        dest='domain_table', type=str, help='目标数据表')
    parser.add_argument('--the_date', default=None,
                        dest='the_date', type=str, help='需要处理的the_date分区')
    parser.add_argument('--file_no', default=None,
                        dest='file_no', type=str, help='需要处理的file_no分区')
    parser.add_argument('--dict_list_file', default=None,
                        dest='dict_list_file', type=str, help='需要加载的字典列表')
    parser.add_argument('--config_name', default=None,
                        dest='config_name', type=str, help='配置文件夹名称')
    parser.add_argument('--class_name', default=None,
                        dest='class_name', type=str, help='类目名称')
    parser.add_argument('--single_partition', default='False',type=str,help='输出表是否是单个file_no分区')
    args = parser.parse_args()
    # 初始化
    data_extractor = DataExtractor(app_name=args.class_name+'_data_extractor')
    dict_list = json.load(open(args.dict_list_file, 'r', encoding='utf-8')
                          ) if args.dict_list_file is not None else []
    domain_extractor = import_fun(
        'config.' + args.config_name+'.domain_extractor.domain_extractor')
    # single_partition = bool(args.single_partition)
    single_partition = True if args.single_partition == 'True' else False
    data_extractor.run(source_table=args.source_table, domain_table=args.domain_table, the_date=args.the_date,
                       file_no=args.file_no, dict_list=dict_list, domain_extractor=domain_extractor,single_partition=single_partition)
    # 结束
    data_extractor.stop()
