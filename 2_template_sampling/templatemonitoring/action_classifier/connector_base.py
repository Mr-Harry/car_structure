import logging
import subprocess
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from string import Template


class BaseSparkConnector(object):
    def __init__(self, app_name,  log_level='INFO'):
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

    def write_table(self, df, dest_table, partition=None):
        df.createOrReplaceTempView("tmp_table")
        if partition is None:
            _sql = "insert overwrite table {0} select * from tmp_table".format(
                dest_table)
        else:
            _sql = "insert overwrite table {0} partition({1}) select * from tmp_table".format(
                dest_table, partition)
        self.spark.sql(_sql)


    def read_table(self, query):
        self.logger.info('将要执行如下HiveSQL query进行数据获取:')
        self.logger.info(query)
        return self.spark.sql(query)
    
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