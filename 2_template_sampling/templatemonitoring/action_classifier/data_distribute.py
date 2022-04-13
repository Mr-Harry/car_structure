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
from connector_base import BaseSparkConnector
from functools import partial
import pandas as pd
import json

# 配置文件信息
os.environ['PYSPARK_PYTHON'] = '/usr/local/python3.7.4/bin/python3'
_sql= Template('''
select 
    row_key,
    app_name,
    suspected_app_name,
    c_id,
    cnt,
    msg,
    class_label,
    industry,
    hash_index_0,
    hash_index_1,
    hash_index_2,
    hash_index_3,
    first_modified
from ${domain_table}
''')


class DataDistributer(BaseSparkConnector):
    def __init__(self, app_name, log_level=None):
        """初始化
        初始化spark
        """
        # 初始化spark
        super().__init__(app_name=app_name, log_level=log_level)
        self.logger.info('预分类模块初始化完毕')

    def run(self, query, hdfs_base_path):
        orig_data = self.read_table(query)
        self.hdfs_dir_rm(hdfs_base_path + '/unclassified')
        self.hdfs_dir_rm(hdfs_base_path + '/model_classified')
        self.spark_to_hdfs(spark_df=orig_data,
                           hdfs_path=hdfs_base_path + '/unclassified')
        # 进行下未分类的数据量的检验 如果无未分类的数据 那么就把相应模型预测结果的目录建好
        cnt = orig_data.count()
        if cnt == 0:
            self.logger.info('query:{0} 无任何数据可以拉取到HDFS'.format(
                query))
            self.hdfs_dir_exist(hdfs_base_path + '/model_classified')


if __name__ == "__main__":
    # 定义参数
    parser = argparse.ArgumentParser(description="预分类模块")
    parser.add_argument('--hdfs_base_path', default=None,
                        dest='hdfs_base_path', type=str, help='模型预测中转目录')
    parser.add_argument('--domain_table', default=None, type=str, help='目标数据获取语句')
    parser.add_argument('--class_name', default=None,
                        dest='class_name', type=str, help='类目名称')

    args = parser.parse_args()
    # 初始化
    # hdfs_base_path = '{0}/{1}_{2}/'.format(args.hdfs_base_path,args.the_date,args.file_no)
    query = _sql.substitute(domain_table=args.domain_table)
    
    data_distributer= DataDistributer(app_name=args.class_name+'_templet_data_distributer')
    data_distributer.run(query,
                        hdfs_base_path=args.hdfs_base_path)
    # 结束
    data_distributer.stop()
