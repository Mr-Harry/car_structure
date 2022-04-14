import os
import logging
import time
import subprocess
from pathlib import Path
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime as dt
from string import Template
from functools import partial
import pandas as pd
import re

domain_source_sql = Template('''
select
    row_key,
    mobile_id,
    event_time,
    app_name,
    suspected_app_name,
    msg,
    main_call_no,
    abnormal_label,
    hashcode,
    the_date,
    ${file_no}
from ${source_table}
where 1=1
and ${the_date_condition}
and ${file_no_condition}
''')

domain_new_source_sql = Template('''
select
    row_key,
    mobile_id,
    event_time,
    app_name,
    suspected_app_name,
    msg,
    main_call_no,
    abnormal_label,
    hashcode,
    the_date,
    file_no,
    domain_extractor(msg,app_name,suspected_app_name,hashcode,abnormal_label) as domain_class
from ${source_table}
where 1=1
and ${the_date_condition}
and ${file_no_condition}
''')

None_source_sql = Template('''
select
    row_key,
    mobile_id,
    event_time,
    app_name,
    suspected_app_name,
    msg,
    main_call_no,
    abnormal_label,
    hashcode,
    the_date,
    ${file_no}
from ${source_table}
where 1=1
and ${the_date_condition}
and ${file_no_condition}
''')


class_source_sql = Template('''
select
    row_key,
    mobile_id,
    event_time,
    app_name,
    suspected_app_name,
    msg,
    main_call_no,
    abnormal_label,
    hashcode,
    class_rule_id,
    class_label,
    class_label_prob,
    ner_label,
    the_date,
    ${file_no}
from ${source_table}
where 1=1
and ${the_date_condition}
and ${file_no_condition}
and ${class_label}
''')

entity_data_sql = Template('''
select
    row_key,
    mobile_id,
    event_time,
    app_name,
    suspected_app_name,
    msg,
    main_call_no,
    class_rule_id,
    class_label,
    class_label_prob,
    abnormal_label,
    ner_label,
    hashcode,
    ${entity_col}
    the_date,
    ${file_no}
from ${source_table}
where 1=1
and ${the_date_condition}
and ${file_no_condition}
''')

monitor_nlp_sql = Template('''
select count(1) as cnt,
${entity_col}
the_date, file_no
from ${source_table}
where 1=1
and ${the_date_condition}
and ${file_no_condition}
group by the_date,file_no
''')

monitor_nlp_mobile_sql = Template('''
select * from (${source_table}) b
''')


sql_template = {
    'domain': domain_source_sql,
    'None': None_source_sql,
    'class': class_source_sql,
    'entity': entity_data_sql,
    'monitor_nlp':monitor_nlp_sql,
    'monitor_nlp_mobile_sql':monitor_nlp_mobile_sql,
    'domain_new':domain_new_source_sql
}


# 目标表结构
domain_schema = StructType([StructField('row_key', StringType(), True),
                            StructField('mobile_id', StringType(), True),
                            StructField('event_time', StringType(), True),
                            StructField('app_name', StringType(), True),
                            StructField('suspected_app_name',
                                        StringType(), True),
                            StructField('msg', StringType(), True),
                            StructField('main_call_no', StringType(), True),
                            StructField('abnormal_label', StringType(), True),
                            StructField('hashcode', StringType(), True),
                            StructField('the_date', StringType(), True),
                            StructField('file_no', StringType(), True)])

pre_class_schema = StructType([StructField('row_key', StringType(), True),
                               StructField('mobile_id', StringType(), True),
                               StructField('event_time', StringType(), True),
                               StructField('app_name', StringType(), True),
                               StructField('suspected_app_name',
                                           StringType(), True),
                               StructField('msg', StringType(), True),
                               StructField('main_call_no', StringType(), True),
                               StructField('abnormal_label',
                                           StringType(), True),
                               StructField('hashcode', StringType(), True),
                               StructField('class_rule_id',
                                           StringType(), True),
                               StructField('class_label', StringType(), True),
                               StructField('ner_label', StringType(), True),
                               StructField('the_date', StringType(), True),
                               StructField('file_no', StringType(), True)])


def entity_col_generator(entity_col_dict):
    entity_col = ''
    # 若字典为空
    if not entity_col_dict:
        return entity_col
    # 按照字典key开始拼接
    for key in entity_col_dict.keys():
        values = entity_col_dict[key]
        entity_col += '{2}({1}) as {0},\n'.format(key, values[0], values[1])
    return entity_col


def monitor_nlp(ner_list):
    entity_col = ''
    if not ner_list:
        return entity_col
    for key in ner_list.keys():
        values = ner_list[key]
        entity_col += "count(case when nvl({0},'') = '' then 1 else null end) as {0},\n".format(values)
    return entity_col


def monitor_mobile_id_nlp(nlp_config,_the_date):
    source_table = ''
    for i,k in enumerate(nlp_config):
        if i != 0:
            source_table += ' union all '
        source_table += 'select mobile_id, \'{0}\' as count_item from {1} where 1=1 and {2} group by mobile_id'.format(k,nlp_config[k],_the_date)
    return source_table
        


class BaseSparkConnector(object):
    def __init__(self, app_name, log_level=None):
        """ 基础Spark连接类
            封装了部分常用功能进来组成基类，方便在后续继承此类构建自己的Spark任务代码类;

            Args:
                app_name: 必填参数，用于标记Spark任务名称;  str
                log_level: 选填参数，用于标记Spark任务的日志等级，只可以为WARN、INFO、DEBUG、ERROR其中一种;  str
        """
        # 配置文件信息 由于经常使用动态分区功能 在这里默认打开
        self.app_name = app_name
        conf = SparkConf().setMaster("yarn").setAppName(self.app_name)
        conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
        conf.set("hive.exec.max.dynamic.partitions","30000")
        # 初始化spark
        self.sc = SparkContext(conf=conf)
        self.spark = SparkSession.builder.config(
            conf=conf).enableHiveSupport().getOrCreate()
        self.sqlContext = SQLContext(self.sc)
        # 设置日志等级
        if log_level:
            if log_level not in ["WARN", "INFO", "DEBUG", "ERROR"]:
                raise ValueError(
                    "detect unexpected log_level: {0}".format(log_level))
            self.sc.setLogLevel(log_level)
        self.logger = self.init_logger()

    def init_logger(self):
        """ 初始化logger
        """
        logger = logging.getLogger(self.app_name)
        logger.setLevel(logging.INFO)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        return logger

    def stop(self):
        """ 退出spark
        """
        self.sc.stop()
        self.spark.stop()

    def _run_cmd(self, command, print_log=True, raise_err=False):
        """ 执行命令行命令

            Args:
                command: 必填参数，需要执行的命令;  str
                pring_log: 选填参数，是否打印返回结果; bool
                raise_err: 选填参数，是否在cmd跑失败的时候抛出异常; bool

            Returns:
                执行命令结果; subprocess.CompletedProcess类
        """
        res = subprocess.run(command, shell=True)
        if print_log:
            self.logger.info(res)
        if raise_err and res.returncode != 0:
            raise ValueError("shell命令执行失败")
        return res

    def save_table(self, df, table_name, partition=None,single_partition=False):
        df.createOrReplaceTempView("tmp_table")
        if single_partition:
            if partition is None:
                _sql = "insert overwrite table {0} partition(file_no) select * from tmp_table distribute by file_no,cast(rand() *20 as int)".format(
                    table_name)
            else:
                _sql = "insert overwrite table {0} partition(file_no,{1}) select * from tmp_table distribute by file_no,{1},cast(rand() *20 as int)".format(
                    table_name, partition)
        else:
            if partition is None:
                _sql = "insert overwrite table {0} partition(the_date,file_no) select * from tmp_table distribute by the_date,file_no,cast(rand() *20 as int)".format(
                    table_name)
            else:
                _sql = "insert overwrite table {0} partition(the_date,file_no,{1}) select * from tmp_table distribute by the_date,file_no,{1},cast(rand() *20 as int)".format(
                    table_name, partition)
        self.logger.info(_sql)
        self.spark.sql(_sql)

    def read_partition(self, source_table, the_date, file_no, sql_class, class_label=None,f_r=True,f_o = True):
        """从指定hive表的指定分区读取数据

            Args:
                source_table: 必填参数，待读取分区的表; str
                the_date: 必填参数，待读取分区; str
                file_no: 选填参数，待读取分区; str
        """
        source_sql = sql_template.get(sql_class, None)
        self.logger.info('将要执行如下sql进行分区数据获取:')
        file_no_condition = "file_no {0} '{1}'".format('regexp' if f_r else '=',
            file_no) if file_no != 'all' else " 1=1 "
        the_date_condition = "the_date regexp '{}'".format(
            the_date) if the_date != 'all' else " 1=1 "
        file_no_f = "'{}' as file_no".format(file_no) if f_o else "file_no"
        if class_label is not None:
            class_label_str = ' 1=1 '
            if type(class_label) is str:
                class_label_str = ' class_label = \'{0}\''.format(class_label)
            if type(class_label) is list:
                class_label_str = ' ('
                for i,class_labe_ in enumerate(class_label):
                    if i != 0:
                        class_label_str += ' or '
                    class_label_str += 'class_label = \'{0}\''.format(class_labe_)
                class_label_str += ')'
            _sql = source_sql.substitute(source_table=source_table, the_date_condition=the_date_condition,
                                         file_no_condition=file_no_condition, class_label=class_label_str,file_no=file_no_f)
        else:
            _sql = source_sql.substitute(
                source_table=source_table, the_date_condition=the_date_condition, file_no_condition=file_no_condition,file_no=file_no_f)

        self.logger.info(_sql)
        return self.spark.sql(_sql)

    def read_partition_nlp_monitor(self, source_table, the_date, file_no, ner_list):
        """从指定hive表的指定分区读取指定实体数据

            Args:
                source_table: 必填参数，待读取分区的表; str
                the_date: 必填参数，待读取分区; str
                file_no: 必填参数，待读取分区; str
                entity_col_dict: 必填参数，需要读取的实体以及名称转换关系字典; dict
        """
        self.logger.info('将要执行如下sql进行数据获取:')
        source_sql = sql_template.get('monitor_nlp', None)
        entity_col = monitor_nlp(ner_list)

        file_no_condition = "file_no = '{}'".format(
            file_no) if file_no != 'all' else " 1=1 "
        the_date_condition = "the_date regexp '{}'".format(
            the_date) if the_date != 'all' else " 1=1 "
        _sql = source_sql.substitute(entity_col=entity_col, source_table=source_table,
                                     the_date_condition=the_date_condition, file_no_condition=file_no_condition)

        self.logger.info(_sql)
        return self.spark.sql(_sql)
    def read_partition_nlp_mobile_id_monitor(self, nlp_config, the_date, file_no):
        """从指定hive表的指定分区读取指定实体数据

            Args:
                source_table: 必填参数，待读取分区的表; str
                the_date: 必填参数，待读取分区; str
                file_no: 必填参数，待读取分区; str
                entity_col_dict: 必填参数，需要读取的实体以及名称转换关系字典; dict
        """
        self.logger.info('将要执行如下sql进行数据获取:')
        source_sql = sql_template.get('monitor_nlp_mobile_sql', None)

        file_no_condition = "file_no = '{}'".format(
            file_no) if file_no != 'all' else " 1=1 "
        the_date_condition = "the_date regexp '{}'".format(
            the_date) if the_date != 'all' else " 1=1 "
        _the_date = the_date_condition + ' and ' + file_no_condition
        source_table = monitor_mobile_id_nlp(nlp_config=nlp_config,_the_date=_the_date)
        _sql = source_sql.substitute(source_table=source_table)

        self.logger.info(_sql)
        return self.spark.sql(_sql)

    def read_partition_entity(self, source_table, the_date, file_no, entity_col_dict,f_r=True,f_o=True):
        """从指定hive表的指定分区读取指定实体数据

            Args:
                source_table: 必填参数，待读取分区的表; str
                the_date: 必填参数，待读取分区; str
                file_no: 必填参数，待读取分区; str
                entity_col_dict: 必填参数，需要读取的实体以及名称转换关系字典; dict
        """
        self.logger.info('将要执行如下sql进行数据获取:')
        source_sql = sql_template.get('entity', None)
        entity_col = entity_col_generator(entity_col_dict)
        file_no_f = "'{}' as file_no".format(file_no) if f_o else "file_no"
        file_no_condition = "file_no {0} '{1}'".format('regexp' if f_r else '=',
            file_no) if file_no != 'all' else " 1=1 "
        the_date_condition = "the_date regexp '{}'".format(
            the_date) if the_date != 'all' else " 1=1 "
        _sql = source_sql.substitute(entity_col=entity_col, source_table=source_table,
                                     the_date_condition=the_date_condition, file_no_condition=file_no_condition,file_no=file_no_f)

        self.logger.info(_sql)
        return self.spark.sql(_sql)

    def hdfs_dir_exist(self, hdfs_path, create=True):
        """ 判断hdfs目录是否存在

            Args:
                hdfs_path: 必填参数，需要检查的hdfs目录;  str
                create: 选填参数，如果不存在是否需要建立; str

            Returns:
                存在返回True，其它返回False; bool
        """
        res = self._run_cmd('hdfs dfs -ls ' + hdfs_path, print_log=True)
        if res.returncode != 0:
            if create:
                self._run_cmd('hdfs dfs -mkdir ' + hdfs_path, print_log=True)
            return False
        return True

    def hdfs_dir_empty(self, hdfs_path):
        """ 判断hdfs目录是否为空

            Args:
                hdfs_path: 必填参数，需要检查的hdfs目录;  str

            Returns:
                非空返回False，其它返回True; bool
        """
        self.logger.info('将要判断如下hdfs目录是否为空: {0}'.format(hdfs_path))
        res = self._run_cmd('hdfs dfs -ls ' + hdfs_path, print_log=True)
        if res.returncode == 0:
            count = len(subprocess.check_output('hdfs dfs -ls ' +
                                                hdfs_path, shell=True).decode().split('\n'))
            if count > 0:
                return False
        return True

    def hdfs_dir_rm(self, hdfs_path):
        """ 删除相应的hdfs目录

            Args:
                hdfs_path: 必填参数，需要删除的hdfs目录;  str
        """
        self.logger.info('将要删除如下hdfs目录: {0}'.format(hdfs_path))
        res = self._run_cmd('hdfs dfs -rm -r ' + hdfs_path, print_log=True)

    def spark_to_hdfs(self, spark_df, hdfs_path, sep='\t'):
        """把spark.DataFrame类的数据拉取到HDFS指定目录上面

            Args:
                spark_df: 必填参数，需要拉取的数据;  spark.DataFrame
                hdfs_path: 必填参数，需要写入的HDFS文件夹路径; str
                sep: 选填参数，数据分隔符; str

            Returns:
                任务执行成功返回True，其它返回False; bool
        """
        self.logger.info('将要将数据写入hdfs目录: {0}'.format(hdfs_path))
        try:
            spark_df.write.mode('overwrite').csv(hdfs_path, sep=sep, quote='')
            return True
        except:
            return False
