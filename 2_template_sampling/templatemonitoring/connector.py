import logging
import subprocess
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from string import Template


class BaseSparkConnector(object):
    def __init__(self, app_name, mysql_config, log_level='INFO'):
        """ 基础Spark连接类
            封装了部分常用功能进来组成基类，方便在后续继承此类构建自己的Spark任务代码类;

            Args:
                app_name: 必填参数，用于标记Spark任务名称;  str
                log_level: 选填参数，用于标记Spark任务的日志等级，只可以为WARN、INFO、DEBUG、ERROR其中一种;  str
        """
        # 配置文件信息 由于经常使用动态分区功能 在这里默认打开
        self.app_name = app_name

        
        #####ONLINE####
        conf = SparkConf().setMaster("yarn").setAppName(self.app_name)
        conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
        conf.set("hive.exec.max.dynamic.partitions","30000")
        # 初始化spark
        self.sc = SparkContext(conf=conf)
        self.spark = SparkSession.builder.config(
            conf=conf).enableHiveSupport().getOrCreate()
        self.sqlContext = SQLContext(self.sc)
        """
        ####OFFLINE####
        conf = SparkConf().setMaster('local[4]').setAppName(self.app_name)
        conf.set('spark.driver.extraClassPath', 'mysql-connector-java-8.0.18.jar')
        self.sc = SparkContext(conf=conf)
        self.spark = SparkSession.builder.config(conf=conf).getOrCreate()
        """

        # 设置日志等级
        if log_level:
            if log_level not in ["DEBUG", "INFO", "WARN", "ERROR"]:
                raise ValueError(
                    "detect unexpected log_level: {0}".format(log_level))
            self.sc.setLogLevel(log_level)
        self.logger = self._init_logger()

        # mysql配置
        self.url = mysql_config['url']
        self.mode = mysql_config['mode']
        self.properties = mysql_config['properties']

    def _init_logger(self):
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

    def run_cmd(self, command, print_log=True, raise_err=False):
        """ 执行命令行命令

            Args:
                command: 必填参数，需要执行的命令;  str
                print_log: 选填参数，是否打印返回结果; bool
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

    def write_table(self, df, dest_table, db_type, partition=None):
        if db_type == 'hive':
            df.createOrReplaceTempView("tmp_table")
            if partition is None:
                _sql = "insert overwrite table {0} select * from tmp_table".format(
                    dest_table)
            else:
                _sql = "insert overwrite table {0} partition({1}) select * from tmp_table".format(
                    dest_table, partition)
            self.spark.sql(_sql)
        elif db_type == 'mysql':
            df.write.jdbc(url=self.url, table=dest_table, mode=self.mode, properties=self.properties)
        else:
            raise TypeError('源表的读取只支持hive或者mysql，请检查配置')

    def read_table(self, query, db_type):
        if db_type == 'hive':
            self.logger.info('将要执行如下HiveSQL query进行数据获取:')
            self.logger.info(query)
            return self.spark.sql(query)
        elif db_type == 'mysql':
            self.logger.info('将要执行如下MySQL query进行数据获取:')
            self.logger.info(query)
            return self.spark.read.jdbc(url=self.url, table=query, properties=self.properties)
        else:
            raise TypeError('源表的读取只支持hive或者mysql，请检查配置')
    
    def run_hivesql(self, sql, raise_nums=3, print_log=True):
        for num in range(raise_nums):
            command = f'''beeline -u "jdbc:hive2://dz-hadoop-dn2:2181,dz-hadoop-dn3:2181,dz-hadoop-dn4:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "{sql}"'''
            res = subprocess.run(command, shell=True)
            if print_log:
                self.logger.info(res)
            if res.returncode == 0:
                print(f"shell命令执行{num+1}次成功")
                return res
        raise ValueError(f"shell命令执行{raise_nums}次失败")
        