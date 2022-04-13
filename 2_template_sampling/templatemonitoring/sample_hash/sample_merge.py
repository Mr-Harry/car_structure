# coding:utf-8
from logging import log
import sys
import re
import json
import pyspark.sql.functions as F
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import month, udf,first,count, lit, when
from pyspark.sql.types import StringType,BooleanType
from cluster import MinHashCluster
from utils import import_fun,Shingler
from functools import partial
import os
from hash import MinHash
from tool import Trie,Tokenizer,SimHash,special_text
from pyspark.sql.types import *
import datetime

def target_data_extractor(row, dict_list, domain_extractor):
    return domain_extractor(row.msg, row.app_name, row.suspected_app_name, row.hashcode, row.abnormal_label, dict_list)


# 目标数据抽取函数封装
def target_data_extractor_generator(dict_list, domain_extractor):
    return partial(target_data_extractor, dict_list=dict_list, domain_extractor=domain_extractor)

mysql_config={
    "url": 'jdbc:mysql://10.10.15.13:3306',
    "mode": 'overwrite'  ,
    "properties": {'user': 'root', 'password': 'VsjbvlpeDfkYiRCY' } # mysql的账户密码
}
kwargs={
    "window": 3,
    "permutations": 32,
    "bands": 4,
    "seed": 1201
}
domain_schema = StructType([StructField('row_key', StringType(), True),
                            StructField('mobile_id', StringType(), True),
                            StructField('app_name', StringType(), True),
                            StructField('suspected_app_name',StringType(), True),
                            StructField('msg', StringType(), True),
                            StructField('abnormal_label', StringType(), True),
                            StructField('hashcode', StringType(), True)])

def hash_main(config_name, start_month, end_month, sample_ratio):
    app_name = config_name+"-Sample"
    cluster = MinHashCluster(app_name,mysql_config,log_level='WARN', **kwargs)
    domain_extractor = import_fun(
        'config_sample.' + config_name+'.domain_extractor.domain_extractor')
    dict_list_file = os.path.join('config_sample', config_name, 'dict_list_file.json')
    dict_list = json.load(open(dict_list_file, 'r', encoding='utf-8')
                          ) if dict_list_file is not None else []
    assert len(start_month)==6
    assert len(end_month)==6
    cluster.logger.warning(f"开始月={start_month}；结束月={end_month}")
    while start_month<=end_month:
        the_month=start_month[:4]+'-'+start_month[4:]
        _sql = "select row_key, mobile_id, app_name,suspected_app_name,msg, abnormal_label,hashcode from preprocess.ds_txt_final_sample where abnormal_label='正常文本' and the_date between '{}-01' and '{}-31'".format(the_month, the_month)
        orig_data = cluster.read_table(_sql, "hive")
        target_data_extractor_func = target_data_extractor_generator(
            dict_list, domain_extractor)
        target_data = cluster.spark.createDataFrame(
            orig_data.rdd.filter(target_data_extractor_func), domain_schema).sample(withReplacement=False,  fraction=float(sample_ratio))
        df_out = target_data.select("row_key", "mobile_id","app_name","suspected_app_name","msg", "hashcode",
                                F.lit(the_month).cast(StringType()).alias('the_month'))  
        df_out.repartition(10).write.format('hive').saveAsTable(f'nlp_dev.{config_name}_sample_hash',  mode="append", partitionBy=['the_month'])
        start_month = str(int(start_month)+1) if int(start_month[4:])<12 else str(int(start_month)+100-11)
        cluster.logger.warning(f"the_month={the_month}")
    cluster.stop()



def merge_main(config_name):
    app_name = config_name+"-Sample_merge"
    wordtxt = 'data/new_word_freq.txt'
    conf = SparkConf().setMaster("yarn").setAppName(app_name).set("hive.exec.dynamic.partition.mode", "nonstrict")  # .set("spark.sql.shuffle.partitions", 1000).set("spark.executor.memory", "2g")
    spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
    trie = Trie(wordtxt, pos=True)
    tokenizer = Tokenizer(trie)
    simhash_udf = udf(lambda x: str(SimHash(tokenizer(x)).value), returnType=StringType())
    df = spark.sql(f"select row_key, mobile_id, app_name, suspected_app_name, msg, hashcode,the_month from nlp_dev.{config_name}_sample_hash")
    df = df.select('row_key','mobile_id','app_name', 'suspected_app_name', 'msg', 'hashcode','the_month', simhash_udf('msg').alias('new_hash'))
    
    from pyspark.sql.window import Window
    w = Window.partitionBy('new_hash')
    df = df.select(*df.columns, F.count("new_hash").over(w).alias('cnt'))
    df.write.saveAsTable(f'nlp_dev.{config_name}_sample_merge', mode='overwrite',partitionBy=['the_month'])
    spark.stop()

def merge_main_v2(config_name):
    app_name = config_name+"-Sample"
    cluster = MinHashCluster(app_name,mysql_config,log_level='WARN', **kwargs)
    _sql = f"select row_key, mobile_id, app_name, suspected_app_name, msg, hashcode, the_month from nlp_dev.{config_name}_sample_hash"
    df_in = cluster.read_table(_sql, "hive")
    hash_udf = _hash_udf(cluster)
    df_in = _hashing( df_in, hash_udf, cluster.bands_index)
    df_in = df_in.select(*df_in.columns,
                                F.lit(None).cast(ShortType()).alias('c_id'))  
    df_in.show()
    cluster.logger.warning(df_in.count())
    df_out = cluster._clustering(df_in, 1)
    df_out.write.saveAsTable(f'nlp_dev.{config_name}_sample_merge', mode='overwrite')
    cluster.stop()


def _hashing(dataframe, hash_udf,bands_index):
    from pyspark.sql.window import Window
    w = Window.partitionBy('meta_hash')
    df = dataframe.select(
                            *dataframe.columns,
                            hash_udf(F.col('msg')).alias('meta_hash')
                         ). \
                   select(
                            *dataframe.columns,
                            'meta_hash',
                            F.count('meta_hash').over(w).alias('cnt'),
                            F.rank().over(w.orderBy(F.length('msg').desc(), 'row_key')).alias('rank')
                         ).filter(F.col('rank') == 1).drop('rank').cache()
    hash_cols = list(map(lambda x: F.col('meta_hash')[x].alias('hash_index_' + str(x)),bands_index))
    cols = df.columns + hash_cols
    return df.select(cols).drop('meta_hash')

def _hash_udf(self):
    
    window, permutations, bands, seed = self.window, self.permutations, self.bands, self.seed
    wordtxt='data/new_word_freq.txt'
    trie = Trie(wordtxt, pos=True)
    tokenizer = Tokenizer(trie)
    # pattern = re.compile(
    #     '[\\s\\d,.<>/?:;\'\"[\\]{}()\\|~!@#$%^&*\\-_=+a-zA-Z]+')
    # proc = lambda x:re.sub(pattern, ' ', special_text().generate_text(x))
    shingler = Shingler(window)

    minhash = MinHash(permutations, bands, seed)
    def minhash_lsh(features):
            return minhash.hashing(features)

    return F.udf(lambda x: minhash_lsh(shingler(tokenizer(x))),
                     returnType=ArrayType(StringType()))


if __name__ == "__main__":
    config_name = sys.argv[1]
    start_month = re.sub('-', '',sys.argv[2])
    end_month = re.sub('-', '', sys.argv[3])
    sample_ratio = sys.argv[4]
    assert len(start_month)==6
    assert len(end_month)==6

    if start_month<end_month:
        hash_main(config_name, start_month, end_month, sample_ratio)
    merge_main(config_name)
    
    # merge_main_v2(config_name)


