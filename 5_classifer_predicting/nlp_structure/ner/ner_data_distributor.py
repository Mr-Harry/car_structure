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
from pyspark.sql.types import StringType
from pyspark.sql import Row
import argparse
from functools import partial
import pandas as pd
import logging
from libs.spark_base_connector import BaseSparkConnector
import json

# 配置文件信息
os.environ['PYSPARK_PYTHON'] = '/usr/local/python3.7.4/bin/python3'

# 正式环境必须指定的参数
# app_name = "insurance_ner_ditributor"


class NERDataDistributor(BaseSparkConnector):
    def __init__(self, app_name, log_level=None):
        """
        实体提取数据分发模块
        Args:
            app_name: 必填参数，用于标记Spark任务名称;  str
            log_level: 选填参数，用于标记Spark任务的日志等级，只可以为WARN、INFO、DEBUG、ERROR其中一种;  str
        """
        # 初始化spark
        super().__init__(app_name=app_name, log_level=log_level)
        # 加载完毕
        self.logger.info('实体提取数据分发模块初始化完成')

    def run(self, class_label, need_ner, hdfs_base_path, classified_table, target_nlp_table, the_date, file_no,single_partition=False):
        self.logger.info('开始处理 the_date:{0} file_no:{1} class_label:{2} 的实体抽取模块数据分发阶段工作'.format(
            the_date, file_no, class_label))
        # 判断是否要进行实体抽取工作 如果是的话导入到hdfs目录当中 否的话直接导入到对应的表当中
        if need_ner == '1':
            self.logger.info('开始处理 the_date:{0} file_no:{1} class_label:{2} 的数据拉取到HDFS的工作'.format(
                the_date, file_no, class_label))
            # 判断目录是否存在
            self.hdfs_dir_exist(hdfs_base_path)
            self.hdfs_dir_exist(hdfs_base_path + '/to_ner/')
            self.hdfs_dir_exist(hdfs_base_path + '/ner_result/')
            class_labels = class_label.split(',')
            if len(class_labels) > 1:
                class_label = class_label.replace(',','__')
                class_label_str = class_labels
            else:
                class_label_str = class_label
            # 删除将要写入目录下对应文件
            is_empty = self.hdfs_dir_rm(
                hdfs_base_path + '/to_ner/' + class_label + '/')
            # 读取数据
            f_r = False if single_partition else True
            f_o = False if single_partition else True
            orig_data = self.read_partition(
                source_table=classified_table, the_date=the_date, file_no=file_no, sql_class='class', class_label=class_label_str,f_r=f_r,f_o=f_o)
            # 写入目录当中
            self.spark_to_hdfs(
                spark_df=orig_data, hdfs_path=hdfs_base_path + '/to_ner/' + class_label + '/')
            # 进行需要模型进行预测的数据量的判断 如果为0那么直接建好相应的目录
            cnt = orig_data.count()
            if cnt == 0:
                self.logger.info('the_date:{0} file_no:{1} class_label:{2} 无任何数据可以拉取到HDFS'.format(
                    the_date, file_no, class_label))
                self.hdfs_dir_exist(
                    hdfs_base_path + '/ner_result/' + class_label + '/')

        else:
            self.logger.info('开始处理 the_date:{0} file_no:{1} class_label:{2} 的数据写入到目标表:{3} 的工作'.format(
                the_date, file_no, class_label, target_nlp_table))
            # 读取数据
            f_r = False if single_partition else True
            f_o = False if single_partition else True
            orig_data = self.read_partition(
                source_table=classified_table, the_date=the_date, file_no=file_no, sql_class='class', class_label=class_label,f_r=f_r,f_o=f_o)
            # 写入目标表
            self.save_table(orig_data, target_nlp_table,single_partition=single_partition)
            

if __name__ == "__main__":
    # 定义参数
    parser = argparse.ArgumentParser(description="实体抽取模块数据分发")
    parser.add_argument('--config', default=None,
                        dest='config', type=str, help='配置参数信息')
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
    data_distributor = NERDataDistributor(
        app_name=args.class_name+'_NER_DataDistributor')
    # 解析配置信息
    single_partition = True if args.single_partition == 'True' else False
    config_dict = json.load(open(args.config, 'r', encoding='utf-8'))
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
        # 运行
        if (ner_dict_list and class_label in ner_dict_list) or not ner_dict_list:
            data_distributor.run(class_label=class_label,
                                need_ner=need_ner_dict.get(class_label),
                                hdfs_base_path=hdfs_base_path,
                                classified_table=config_dict.get(
                                    'classified_table'),
                                target_nlp_table=nlp_table_dict.get(class_label),
                                the_date=args.the_date,
                                file_no=args.file_no,single_partition=single_partition)
    # 结束
    data_distributor.stop()
