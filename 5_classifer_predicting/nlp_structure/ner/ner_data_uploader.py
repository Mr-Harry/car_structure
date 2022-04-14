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
import json

ABSPATH = os.path.dirname(os.path.abspath(__file__))
# 配置文件信息
os.environ['PYSPARK_PYTHON'] = '/usr/local/python3.7.4/bin/python3'


# 输出到实体结果表的基本字段
base_col = ["row_key", "mobile_id", "event_time",  "app_name", "suspected_app_name", "msg", "main_call_no",
            "abnormal_label", "hashcode", "class_rule_id", "class_label", "class_label_prob", "ner_label", "the_date", "file_no"]


class NerDataUploader(BaseSparkConnector):
    def __init__(self, app_name, log_level=None):
        """
        实体提取成果上传模块
        Args:
            app_name: 必填参数，用于标记Spark任务名称;  str
            log_level: 选填参数，用于标记Spark任务的日志等级，只可以为WARN、INFO、DEBUG、ERROR其中一种;  str
        """
        # 初始化spark
        super().__init__(app_name=app_name, log_level=log_level)
        # 加载完毕
        self.logger.info('实体提取成果上传模块初始化完成')

    def run(self, the_date, file_no, class_label, target_nlp_table, hdfs_base_path, col_config_dict,single_partition=False):
        # 判断目录是否为空
        class_label_str = class_label.replace(',','__') if ',' in class_label else class_label
        is_empty = self.hdfs_dir_empty(
            hdfs_base_path + '/ner_result/' + class_label_str + '/')
        if is_empty:
            self.logger.info("needed path is non-existent or empty :{0}".format(
                hdfs_base_path + '/ner_result/' + class_label_str + '/'))
        # 删除模型预测目录下面的json结尾的文件
        self._run_cmd('hdfs dfs -rm ' + hdfs_base_path +
                      '/ner_result/' + class_label_str + '/*.json')
        # 读取文件内容
        ner_result = self.sc.textFile(
            hdfs_base_path + '/ner_result/' + class_label_str + '/')
        # 转成DataFrame
        schema = StructType()
        # 通过查询配置文件的方式拼出dataframe的结构
        col_list_out = base_col[:-2]+col_config_dict.get(
            class_label, {}).get('ner_list', [])+base_col[-2:]
        col_list_in = base_col + \
            col_config_dict.get(class_label, {}).get('ner_list', [])
        col_list = []
        for col_in, col_out in zip(col_list_in, col_list_out):
            in_col = col_config_dict.get(class_label, {}).get(
                'ner2nlp_col', {}).get(col_in, col_in)
            out_col = col_config_dict.get(class_label, {}).get(
                'ner2nlp_col', {}).get(col_out, col_out)
            col_list.append(out_col)
            schema.add(in_col, StringType(), False)
        ner_result_df = self.spark.createDataFrame(
            ner_result.map(lambda x: x.split('\t')), schema)
        # col_list = col_list[]
        ner_result_df = ner_result_df.select(*col_list)
        # 替换class_label_prob的字段类型
        ner_result_df = ner_result_df.withColumn(
            'class_label_prob', ner_result_df['class_label_prob'].cast(DoubleType()))
        # 写入特定表中
        self.save_table(ner_result_df, target_nlp_table,single_partition=single_partition)
        # 上传完数据后删除相应的文件
        subprocess.run('hdfs dfs -rm -r ' + hdfs_base_path +
                       '/ner_result/' + class_label_str + '/', shell=True)


if __name__ == "__main__":
    # 定义参数
    parser = argparse.ArgumentParser(description="实体成果上传模块")
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
    parser.add_argument('--single_partition', default='False',type=str,help='输出表是否是单个file_no分区')
    parser.add_argument('--ner_dict',type=str,default='all',help='自定义需要跑的NER类目')
    args = parser.parse_args()
    # 初始化
    data_uploader = NerDataUploader(
        app_name=args.class_name+'_NER_DataUploader')
    # 解析配置信息
    single_partition = True if args.single_partition == 'True' else False
    config_dict = json.load(open(args.config, 'r', encoding='utf-8'))
    col_config_dict = json.load(open(args.col_config, 'r', encoding='utf-8'))
    the_date = args.the_date
    file_no = args.file_no
    nlp_table_dict = config_dict.get('nlp_table_dict', {})
    need_ner_dict = config_dict.get('need_ner_dict', {})
    ner_model_dict = config_dict.get('ner_model_dict', {})
    hdfs_base_path = '{0}/{1}_{2}/'.format(config_dict.get(
        'hdfs_base_path'), args.the_date, args.file_no)
    ner_dict_list = args.ner_dict.split(',') if args.ner_dict != 'all' else []
    # 对不同的类目进行不同的处理
    for class_label in nlp_table_dict.keys():
        # 如果需要进行实体抽取
        if ((ner_dict_list and class_label in ner_dict_list) or not ner_dict_list) and need_ner_dict.get(class_label) == '1':
            # 把数据写入到表中
            data_uploader.run(the_date=args.the_date, file_no=args.file_no,
                            class_label=class_label, target_nlp_table=nlp_table_dict.get(class_label), hdfs_base_path=hdfs_base_path, col_config_dict=col_config_dict,single_partition=single_partition)
    # 结束
    data_uploader.stop()
