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
from connector_base import BaseSparkConnector

# 配置文件信息
os.environ['PYSPARK_PYTHON'] = '/usr/local/python3.7.4/bin/python3'

# 输出到分类结果表的格式
msg_record =Row(
    "row_key",
    "msg",
    "app_name",
    "suspected_app_name",
    "c_id",
    "cnt",
    "class_label",
    "industry",
    "industry_label",
    "industry_label_prob",
    "hash_index_0",
    "hash_index_1",
    "hash_index_2",
    "hash_index_3",
    "first_modified"
)

schema = StructType(
    [
        StructField("row_key",StringType(), True),
        StructField("msg",StringType(), True),
        StructField("app_name",StringType(), True),
        StructField("suspected_app_name",StringType(), True),
        StructField("c_id",StringType(), True),
        StructField("cnt",LongType(), True),
        StructField("class_label",StringType(), True),
        StructField("industry",StringType(), True),
        StructField("industry_label",StringType(), True),
        StructField("industry_label_prob",DoubleType(), True),
        StructField("hash_index_0",StringType(), True),
        StructField("hash_index_1",StringType(), True),
        StructField("hash_index_2",StringType(), True),
        StructField("hash_index_3",StringType(), True),
        StructField("first_modified",StringType(), True)
    ]
)



# 把模型处理成果单行文本转成row


def model_line2row(line):
    # 模型相比于预处理的结果会在尾部增加预测的label和prob两列
    infos = line.split('\t')
    # 判断下我们需要的第14个标签在不在 在的话就报错
    industry_label = infos[13]
    industry_prob = float(infos[14])
    c_id=None
    cnt= int(infos[4])
    return msg_record(infos[0], infos[5], infos[1], infos[2], c_id, cnt, infos[6], infos[7],  industry_label, industry_prob, infos[8],infos[9],infos[10],infos[11],infos[12])


class ClassDataUploader(BaseSparkConnector):

    def __init__(self, app_name, log_level=None):
        """初始化
        初始化spark
        """
        # 初始化spark
        super().__init__(app_name=app_name, log_level=log_level)
        self.logger.info('分类上传模块初始化完毕')

    def run(self, hdfs_base_path, classified_table, partition=None):
        
        is_empty = self.hdfs_dir_empty(
            hdfs_base_path + '/model_classified/')
        if is_empty:
            self.logger.info(
                "needed path is non-existent or empty :{0}".format(hdfs_base_path))
            # raise ValueError("needed path is non-existent or empty :{0}".format(hdfs_base_path))
        # 删除模型预测目录下面的json结尾的文件
        self._run_cmd('hdfs dfs -rm ' + hdfs_base_path +
                      '/model_classified/*.json')
        # 读取文件内容
        model_classified_data = self.sc.textFile(
            hdfs_base_path + '/model_classified/')
        # 转成DataFrame
        model_classified_df = self.spark.createDataFrame(
            model_classified_data.map(model_line2row), schema)

        self.write_table(model_classified_df, classified_table, partition=partition)


if __name__ == "__main__":
    # 定义参数
    parser = argparse.ArgumentParser(description="分类成果上传模块")
    parser.add_argument('--hdfs_base_path', default=None,
                        dest='hdfs_base_path', type=str, help='跟模型服务交互的HDFS目录')
    parser.add_argument('--classified_table', default=None,
                        dest='classified_table', type=str, help='分类成果表')
    parser.add_argument('--class_name', default=None,
                        dest='class_name', type=str, help='类目名称')

    args = parser.parse_args()
    
    # 初始化
    data_uploader = ClassDataUploader(
        app_name=args.class_name+'_ClassDataUploader')
    # 运行
    data_uploader.run(hdfs_base_path=args.hdfs_base_path,
                      classified_table=args.classified_table,
                      partition="first_modified")
    # 结束
    data_uploader.stop()
