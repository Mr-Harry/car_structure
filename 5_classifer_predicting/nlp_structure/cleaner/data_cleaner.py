import argparse
import json
import logging
import os
import re
import subprocess
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
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window
from libs.utils import import_fun, import_module

ABSPATH = os.path.dirname(os.path.abspath(__file__))
# 配置文件信息
os.environ['PYSPARK_PYTHON'] = '/usr/local/python3.7.4/bin/python3'


def clean_udf_generator(dict_list, _fun):
    fun = _fun(dict_list)
    return fun


class DataCleaner(BaseSparkConnector):
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
        self.logger.info('数据清洗模块初始化完成')

    def run(self, the_date, file_no, class_label, class_label_ch_full, target_nlp_table, target_dwb_table, col_config_dict, clean_udf_dict, drop_column, dict_list,single_partition=False):
        # 获取实体字典
        entity_col_dict = col_config_dict.get(
            class_label, {}).get('nlp2dwb', {})

        clean_udf_dict1 = clean_udf_dict.get('clean_udf_dict1', {})
        clean_udf_dict2 = clean_udf_dict.get('clean_udf_dict2', {})
        clean_udf_dict3 = clean_udf_dict.get('clean_udf_dict3', {})
        # filter_udf_dict = clean_udf_dict.get('filter_udf_dict', {})

        clean_udf_dict_class = clean_udf_dict.get('clean_udf_dict_class', {})

        for k in clean_udf_dict_class.keys():
            keynum = clean_udf_dict_class[k][0]
            _func = clean_udf_generator(dict_list, clean_udf_dict_class[k][1])
            if keynum == 1:
                clean_udf_dict1[k] = _func

            if keynum == 2:
                clean_udf_dict2[k] = _func

            if keynum == 3:
                clean_udf_dict3[k] = _func

        for key in clean_udf_dict1.keys():
            self.spark.udf.register(
                key, lambda x: clean_udf_dict1[key](x), returnType=StringType())

        for key in clean_udf_dict2.keys():
            self.spark.udf.register(
                key, lambda x, y: clean_udf_dict2[key](x, y), returnType=StringType())

        for key in clean_udf_dict3.keys():
            self.spark.udf.register(
                key, lambda x, y, z: clean_udf_dict3[key](x, y, z), returnType=StringType())

        # 读取数据,并拼接sql进行实体清洗
        f_r = False if single_partition else True
        f_o = False if single_partition else True
        orig_data = self.read_partition_entity(
            source_table=target_nlp_table, the_date=the_date, file_no=file_no, entity_col_dict=entity_col_dict,f_r=f_r,f_o=f_o)
        orig_data.cache()
        # # 替换特定列
        filter_ = col_config_dict.get(
            class_label, {}).get('rule_filter', [])

        if filter_:
            orig_data.createOrReplaceTempView('tmp_table_filter')
            filter_sql = r"select * from tmp_table_filter where {0}({1}) = 'true'".format(
                filter_[1], filter_[0])
            self.logger.info('开始使用自定义UDF函数过滤数据：\n\n' + filter_sql+'\n\n')
            orig_data = self.spark.sql(filter_sql)
            orig_data.cache()

        correct_data = orig_data.withColumn(
            'class_label', lit(class_label_ch_full))
        # 删除特定列
        correct_data = correct_data.drop('class_rule_id')
        correct_data = correct_data.drop('correct_class_label')
        correct_data = correct_data.drop('hashcode')
        correct_data = correct_data.drop('ner_label')
        correct_data = correct_data.drop('abnormal_label')
        if drop_column:
            for _column in drop_column:
                correct_data =correct_data.drop(_column)

        self.save_table(correct_data, target_dwb_table,single_partition=single_partition)


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

    parser.add_argument('--test_class_name', default=None,
                        dest='test_class_name', type=str, help='单独测试某个细类目')

    args = parser.parse_args()
    # 解析配置信息
    config_dict = json.load(open(args.config, 'r', encoding='utf-8'))
    col_config_dict = json.load(open(args.col_config, 'r', encoding='utf-8'))
    the_date = args.the_date
    file_no = args.file_no
    nlp_table_dict = config_dict.get('nlp_table_dict', {})
    dwb_table_dict = config_dict.get('dwb_table_dict', {})
    full_label_dict = config_dict.get('full_label_dict', {})
    single_partition = config_dict.get('single_partition',False)
    dict_list = json.load(open(args.dict_list_file, 'r', encoding='utf-8')
                          ) if args.dict_list_file is not None else []
    drop_column = config_dict.get('drop_column', [])
    # 初始化
    data_cleaner = DataCleaner(app_name=args.class_name+'_data_cleaner')
    clean_udf_dict = import_fun(
        'config.' + args.config_name+'.clean_udf.clean_udf_dict')

    # 循环进行数的清洗工作

    class_labels = [args.test_class_name] if args.test_class_name is not None and args.test_class_name in nlp_table_dict.keys() else nlp_table_dict.keys()

    for class_label in class_labels:
        nlp_table = nlp_table_dict.get(class_label)
        dwb_table = dwb_table_dict.get(class_label)
        full_label = full_label_dict.get(class_label, class_label)
        # 运行
        data_cleaner.run(class_label=class_label,
                         class_label_ch_full=full_label,
                         target_nlp_table=nlp_table,
                         target_dwb_table=dwb_table,
                         the_date=args.the_date,
                         file_no=args.file_no,
                         col_config_dict=col_config_dict,
                         clean_udf_dict=clean_udf_dict,
                         drop_column=drop_column,
                         dict_list=dict_list,
                         single_partition=single_partition
                         )
    # 结束
    data_cleaner.stop()
