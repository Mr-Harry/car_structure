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
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window
from libs.utils import import_fun, import_module

ABSPATH = os.path.dirname(os.path.abspath(__file__))
# 配置文件信息
os.environ['PYSPARK_PYTHON'] = '/usr/local/python3.7.4/bin/python3'


class MonitorSyn(BaseSparkConnector):
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

    def run_data(self, table):
        df = self.read_partition(table, 'all', 'all', 'None')
        data = df.collect()
        count_dict = {}
        cout_label_dict = {}
        cout_label_ner_dict = {}
        for item in data:
            if item[-3] not in count_dict:
                count_dict[item[-3]] = {}
            if item[-1] not in count_dict[item[-3]]:
                count_dict[item[-3]][item[-1]] = 0

            if item[-3] not in cout_label_dict:
                cout_label_dict[item[-3]] = {}
            if item[-1] not in cout_label_dict[item[-3]]:
                cout_label_dict[item[-3]][item[-1]] = {}

            if item[-3] not in cout_label_ner_dict:
                cout_label_ner_dict[item[-3]] = {}
            if item[-1] not in cout_label_ner_dict[item[-3]]:
                cout_label_ner_dict[item[-3]][item[-1]] = {}
            label_data = json.loads(item[1])
            for k in label_data:
                if k not in cout_label_dict[item[-3]][item[-1]]:
                    cout_label_dict[item[-3]][item[-1]][k] = 0
                cout_label_dict[item[-3]][item[-1]][k] += label_data[k]
            ner_data = json.loads(item[2])
            for k in ner_data:
                if k not in cout_label_ner_dict[item[-3]][item[-1]]:
                    cout_label_ner_dict[item[-3]][item[-1]][k] = {}
                for kk in ner_data[k]:
                    if kk not in cout_label_ner_dict[item[-3]][item[-1]][k]:
                        cout_label_ner_dict[item[-3]][item[-1]][k][kk] = 0
                    cout_label_ner_dict[item[-3]][item[-1]][k][kk] += ner_data[k][kk]
            count_dict[item[-3]][item[-1]] += item[0]
        cout_label = []
        for k in cout_label_dict:
            for kk in cout_label_dict[k]:
                for kkk in cout_label_dict[k][kk]:
                    cout_label.append((kkk,kk,'count',cout_label_dict[k][kk][kkk],k))
                    

        count_label_ner = []
        for k in cout_label_ner_dict:
            _d = cout_label_ner_dict[k]
            for kk in _d:
                __d = _d[kk]
                for kkk in __d:
                    for kkkk in __d[kkk]:
                        p = 1 - float(__d[kkk][kkkk])/float(cout_label_dict[k][kk][kkk]) if cout_label_dict[k][kk][kkk] > 0 else 0
                        count_label_ner.append((kkk,kk,kkkk,p,k))
                        
        dc = pd.DataFrame(count_label_ner,columns=['细类目','大类目','字段','数据量','日期'])
        dd = pd.DataFrame(cout_label,columns=['细类目','大类目','字段','数据量','日期'])
        dcd = pd.concat([dc,dd],axis=0)
        new_df = self.spark.createDataFrame(dcd)
        new_df.write.jdbc(url='jdbc:mysql://10.10.10.50:3306/nlp_structure_old',
                          mode='overwrite',
                          table='test',
                          properties={
                              'user': 'sf', 'password': 'KiYQ0g&CJgo!cgWR', 'driver': 'com.mysql.jdbc.Driver'}
                      )

    def run_mobile(self,table):
        df = self.read_partition(table, 'all', 'all', 'None')

        df.write.jdbc(url='jdbc:mysql://10.10.10.50:3306/nlp_structure_old',
                          mode='overwrite',
                          table='test_mobile',
                          properties={
                              'user': 'sf', 'password': 'KiYQ0g&CJgo!cgWR', 'driver': 'com.mysql.jdbc.Driver'}
                      )




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="数据清洗模块")
    parser.add_argument('--type', default='data',
                        dest='type', type=str, help='统计类型，data,mobile')
    args = parser.parse_args()
    monitor_syn = MonitorSyn(app_name='Monitor Syn')
    if args.type == 'data':
        monitor_syn.run_data(table='nlp_dev.nlp_structure_count')
    elif args.type == 'mobile':
        monitor_syn.run_mobile(table='nlp_dev.nlp_structure_countid')
    # 结束
    monitor_syn.stop()
