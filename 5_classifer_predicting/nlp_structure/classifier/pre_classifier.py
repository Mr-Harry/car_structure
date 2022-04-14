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
from libs.spark_base_connector import BaseSparkConnector, pre_class_schema
from libs.utils import rule_classifier, hash_classifier
from functools import partial
import pandas as pd
import json

# 配置文件信息
os.environ['PYSPARK_PYTHON'] = '/usr/local/python3.7.4/bin/python3'


################################################## 主体数据流程逻辑 #############################################
# 预分类结果表字段配置
msg_record = Row("row_key", "mobile_id", "event_time",  "app_name", "suspected_app_name", "msg", "main_call_no",
                 "abnormal_label", "hashcode", "class_rule_id", "class_label", "ner_label", "the_date", "file_no")

# 预分类模块当行分类函数


def row_classifier(row, classifier_hash_info_dict, classifier_rule_info_dict):
    # 先进行hash碰撞
    class_label, ner_label = hash_classifier(
        row.hashcode, classifier_hash_info_dict)
    # 如果有匹配到 那么class_rule_id标记为0
    if class_label != '未识别':
        class_rule_id = '0'
    # 如果没有匹配到 那么进行规则分类
    else:
        class_rule_id, class_label, ner_label = rule_classifier(
            row.msg, classifier_rule_info_dict)
    return msg_record(row.row_key, row.mobile_id, row.event_time, row.app_name, row.suspected_app_name,
                      row.msg, row.main_call_no, row.abnormal_label,  row.hashcode,
                      class_rule_id, class_label, ner_label, row.the_date, row.file_no)

# 预分类模块当行分类函数的加载函数


def row_classifier_generator(classifier_hash_info_dict, classifier_rule_info_dict):
    return partial(row_classifier, classifier_hash_info_dict=classifier_hash_info_dict, classifier_rule_info_dict=classifier_rule_info_dict)


class PreClassifier(BaseSparkConnector):
    def __init__(self, app_name, log_level=None):
        """初始化
        初始化spark
        """
        # 初始化spark
        super().__init__(app_name=app_name, log_level=log_level)
        self.logger.info('预分类模块初始化完毕')

    def run(self, domain_table, the_date, file_no, hdfs_base_path, dict_list,single_partition=False):
        classifier_hash_info_dict = dict_list.get('classifier_hash', {})
        classifier_rule_info_dict = dict_list.get('classifier_rule', {})
        self.logger.info('开始如下参数的预分类工作 {0} {1} {2}'.format(
            domain_table, the_date, file_no))
        # 判断目录是否存在
        self.hdfs_dir_exist(hdfs_base_path + '/' + file_no)
        # 从目标数据表获取数据
        f_r = False if single_partition else True
        f_o = False if single_partition else True
        orig_data = self.read_partition(
            source_table=domain_table, the_date=the_date, file_no=file_no, sql_class='None',f_r=f_r,f_o=f_o)
        # 进行过滤
        row_classifier_func = row_classifier_generator(
            classifier_hash_info_dict=classifier_hash_info_dict, classifier_rule_info_dict=classifier_rule_info_dict)
        target_data = self.spark.createDataFrame(
            orig_data.rdd.map(row_classifier_func), pre_class_schema)
        # 进行中间表的存储
        target_data.cache()
        # self.save_table(target_data,pre_classified_table)
        # 把拆分成以分类和未分类，写入到相应的hdfs目录上面
        classified_data = target_data[col("class_rule_id") != '-1']
        unclassified_data = target_data[col("class_rule_id") == '-1']
        # 在写之前删除相应目录下面的内容
        self.hdfs_dir_rm(hdfs_base_path + '/pre_classified')
        self.hdfs_dir_rm(hdfs_base_path + '/unclassified')
        self.hdfs_dir_rm(hdfs_base_path + '/model_classified')
        self.spark_to_hdfs(spark_df=classified_data,
                           hdfs_path=hdfs_base_path + '/pre_classified')
        self.spark_to_hdfs(spark_df=unclassified_data,
                           hdfs_path=hdfs_base_path + '/unclassified')
        self.logger.info('完成如下参数的预分类工作 {0} {1} {2}'.format(
            domain_table, the_date, file_no))
        # 进行下未分类的数据量的检验 如果无未分类的数据 那么就把相应模型预测结果的目录建好
        unclassified_cnt = unclassified_data.count()
        if unclassified_cnt == 0:
            self.logger.info('如下参数的预分类工作 {0} {1} {2} 的未分类部分数量为0'.format(
                domain_table, the_date, file_no))
            self.hdfs_dir_exist(hdfs_base_path + '/model_classified')


if __name__ == "__main__":
    # 定义参数
    parser = argparse.ArgumentParser(description="预分类模块")
    parser.add_argument('--hdfs_base_path', default=None,
                        dest='hdfs_base_path', type=str, help='模型预测中转目录')
    parser.add_argument('--domain_table', default=None,
                        dest='domain_table', type=str, help='目标数据表')
    parser.add_argument('--the_date', default=None,
                        dest='the_date', type=str, help='需要处理的the_date分区')
    parser.add_argument('--file_no', default=None,
                        dest='file_no', type=str, help='需要处理的file_no分区')
    parser.add_argument('--dict_list_file', default=None,
                        dest='dict_list_file', type=str, help='需要加载的字典列表')
    parser.add_argument('--class_name', default=None,
                        dest='class_name', type=str, help='类目名称')
    parser.add_argument('--single_partition', default='False',type=str,help='输出表是否是单个file_no分区')
    # parser.add_argument('--config',default=None, dest = 'config',type=str, help='目标获取筛选函数名')
    args = parser.parse_args()
    # 初始化
    # hdfs_base_path = '{0}/{1}_{2}/'.format(args.hdfs_base_path,args.the_date,args.file_no)
    dict_list = json.load(open(args.dict_list_file, 'r', encoding='utf-8')
                          ) if args.dict_list_file is not None else {}
    pre_classifier = PreClassifier(app_name=args.class_name+'_pre_classifier')
    pre_classifier.run(domain_table=args.domain_table, the_date=args.the_date,
                       file_no=args.file_no, hdfs_base_path=args.hdfs_base_path, dict_list=dict_list,single_partition=args.single_partition)
    # 结束
    pre_classifier.stop()
