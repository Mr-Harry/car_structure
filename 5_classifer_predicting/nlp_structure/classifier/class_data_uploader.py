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

# 输出到分类结果表的格式
msg_record = Row("row_key", "mobile_id", "event_time",  "app_name", "suspected_app_name", "msg", "main_call_no",
                 "abnormal_label", "hashcode", "class_rule_id", "ner_label", "class_label_prob", "the_date", "file_no", "class_label")
schema = StructType([StructField('row_key', StringType(), True),
                     StructField('mobile_id', StringType(), True),
                     StructField('event_time', StringType(), True),
                     StructField('app_name', StringType(), True),
                     StructField('suspected_app_name', StringType(), True),
                     StructField('msg', StringType(), True),
                     StructField('main_call_no', StringType(), True),
                     StructField('abnormal_label', StringType(), True),
                     StructField('hashcode', StringType(), True),
                     StructField('class_rule_id', StringType(), True),
                     StructField('ner_label', StringType(), True),
                     StructField('class_label_prob', DoubleType(), True),
                     StructField('the_date', StringType(), True),
                     StructField('file_no', StringType(), True),
                     StructField('class_label', StringType(), True)])


# 把预分类单行文本封装成row
def line2row(line):
    infos = line.split('\t')
    return msg_record(infos[0], infos[1], infos[2], infos[3], infos[4], infos[5], infos[6], infos[7], infos[8], infos[9], infos[11], 1.0, infos[12], infos[13], infos[10])

# 把模型处理成果单行文本转成row


def model_line2row(line):
    # 模型相比于预处理的结果会在尾部增加预测的label和prob两列
    infos = line.split('\t')
    # 判断下我们需要的第14个标签在不在 在的话就报错
    model_label = infos[14]
    model_prob = float(infos[15])
    return msg_record(infos[0], infos[1], infos[2], infos[3], infos[4], infos[5], infos[6], infos[7], infos[8], '-1', 'normal', model_prob, infos[12], infos[13], model_label)


class ClassDataUploader(BaseSparkConnector):

    def __init__(self, app_name, log_level=None):
        """初始化
        初始化spark
        """
        # 初始化spark
        super().__init__(app_name=app_name, log_level=log_level)
        self.logger.info('分类上传模块初始化完毕')

    def run(self, hdfs_base_path, classified_table,single_partition=False):
        # 判断目录是否为空 视预分类模块是否存在要进行一定的代码调整
        is_empty = False
        is_empty = is_empty | self.hdfs_dir_empty(
            hdfs_base_path + '/pre_classified/')
        is_empty = is_empty | self.hdfs_dir_empty(
            hdfs_base_path + '/model_classified/')
        if is_empty:
            self.logger.info(
                "needed path is non-existent or empty :{0}".format(hdfs_base_path))
            # raise ValueError("needed path is non-existent or empty :{0}".format(hdfs_base_path))
        # 删除模型预测目录下面的json结尾的文件
        self._run_cmd('hdfs dfs -rm ' + hdfs_base_path +
                      '/model_classified/*.json')
        # 读取文件内容
        pre_classified_data = self.sc.textFile(
            hdfs_base_path + '/pre_classified/')
        model_classified_data = self.sc.textFile(
            hdfs_base_path + '/model_classified/')
        # 转成DataFrame
        pre_classified_df = self.spark.createDataFrame(
            pre_classified_data.map(line2row), schema)
        model_classified_df = self.spark.createDataFrame(
            model_classified_data.map(model_line2row), schema)
        classified_df = pre_classified_df.union(model_classified_df)
        self.save_table(classified_df, classified_table, 'class_label',single_partition=single_partition)


if __name__ == "__main__":
    # 定义参数
    parser = argparse.ArgumentParser(description="分类成果上传模块")
    parser.add_argument('--hdfs_base_path', default=None,
                        dest='hdfs_base_path', type=str, help='跟模型服务交互的HDFS目录')
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
    data_uploader = ClassDataUploader(
        app_name=args.class_name+'_ClassDataUploader')
    # 运行
    data_uploader.run(hdfs_base_path=args.hdfs_base_path,
                      classified_table=args.classified_table,single_partition=single_partition)
    # 结束
    data_uploader.stop()
