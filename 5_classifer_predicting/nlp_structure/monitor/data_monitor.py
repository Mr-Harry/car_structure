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
# count,class_label_count,class_label_ner_count,the_date,file_no,class_name
    def run(self, nlp_table_dict,full_label_dict, col_config_dict,the_date, file_no,class_name,dt):
        count_dict = {}
        out_dict = []
        _cnt = {}
        _class_count = {}
        _class_ner_count = {}
        _class_ner_count_d = {}
        _partition_ = []
        _class_count_d = {}
        for _class_name in nlp_table_dict:
            _table = nlp_table_dict[_class_name]
            _ner_list = col_config_dict[_class_name]['ner2nlp_col']
            _data = self.read_partition_nlp_monitor(_table,the_date,file_no,_ner_list)
            count_dict[_class_name] = _data.toJSON().collect()
            _class_count_d.setdefault(_class_name,0)
            _class_ner_count_d.setdefault(_class_name,{})
            for k in _ner_list.keys():
                _class_ner_count_d[_class_name].setdefault(_ner_list[k],0)

        
        # for _the_date_file_no in 
        for _class_name in count_dict.keys():
            for _data in count_dict[_class_name]:
                _data = json.loads(_data)
                partition = (_data['the_date'],_data['file_no'])
                if partition not in _partition_:
                    _partition_.append(partition)
                if partition not in _cnt:
                    _cnt[partition] = 0
                _cnt[partition] += _data['cnt']
                if partition not in _class_count:
                    _class_count[partition] = copy.deepcopy(_class_count_d)
                _class_count[partition][_class_name] = _data['cnt']
                del _data['cnt']
                del _data['the_date']
                del _data['file_no']
                if partition not in _class_ner_count:
                    _class_ner_count[partition] = copy.deepcopy(_class_ner_count_d)
                _class_ner_count[partition][_class_name].update(_data)
        sqls = []
        for partition in _partition_:
            _count = _cnt[partition]
            _class_label_count = json.dumps(_class_count[partition],ensure_ascii = False)
            _class_label_ner_count = json.dumps(_class_ner_count[partition],ensure_ascii=False)
            _the_date = partition[0]
            _file_no = partition[1]
            _class_name = class_name
            _sql = "INSERT INTO nlp_structure_old.nlp_count_test (count,class_label_count,class_label_ner_count,the_date,file_no,class_name)VALUES ({0}, '{1}', '{2}', '{3}', '{4}', '{5}') ON DUPLICATE KEY UPDATE count = {0},class_label_count = '{1}',class_label_ner_count = '{2}';".format(_count,_class_label_count,_class_label_ner_count,_the_date,_file_no,_class_name)
            sqls.append(_sql)
        if len(sqls) > 0:
            r = dt.run_sql(sqls)
            print(r)
            if r == False:
                raise Exception("任务失败！") 


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
                    class_name=args.class_name,
                    dt = dt)
    # 结束
    data_monitor.stop()
