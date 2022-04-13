import os
import re
import itertools
from pyspark.sql import Row
import pyspark.sql.functions as F
from pyspark.sql.functions import coalesce, udf, first, lit, collect_set, concat_ws, md5, concat
from pyspark.sql.window import Window
from pyspark.sql.types import *
from monitor import HighFreqTemplate
import re
from connector import BaseSparkConnector
from utils import Trie, Tokenizer, Shingler, add
from hash import MinHash
from graphframes import GraphFrame

# def udf_type(return_type):
#     def _udf_type_wrapper(func):
#         return F.udf(func, return_type)
#     return _udf_type_wrapper



class MinHashCluster(BaseSparkConnector):
    __slots__ = ('permutations', 'bands', 'seed', 'window')

    def __init__(self, app_name, mysql_config, log_level=None, **kwargs):
        super(MinHashCluster, self).__init__(app_name=app_name, mysql_config=mysql_config, log_level=log_level)
        for k, v in kwargs.items():
            setattr(self, k, v)
        self.hash_udf = self._hash_udf()
        self.bands_index = range(self.bands)

    def _hashing(self, dataframe):
        w = Window.partitionBy('meta_hash')
        df = dataframe.select(
                                *dataframe.columns,
                                self.hash_udf(F.col('msg')).alias('meta_hash')
                             ). \
                       select(
                                *dataframe.columns,
                                'meta_hash',
                                F.count('meta_hash').over(w).alias('cnt'),
                                F.row_number().over(w.orderBy(F.length('msg').desc())).alias('rank')
                             ).filter(F.col('rank') == 1).drop('rank')
        hash_cols = list(map(lambda x: F.col('meta_hash')[x].alias('hash_index_' + str(x)), self.bands_index))

        cols = df.columns + hash_cols
        return df.select(cols).drop('meta_hash')


    def _clustering(self, dataframe, anchor, degree=1):

        if degree > self.bands:
            raise ValueError("Expecting degree less than %d, got %d" % (self.bands, degree))
        if not isinstance(degree, int):
            raise ValueError("Expecting degree is type of int, got %s" % type(degree))

        original_df = dataframe.withColumnRenamed('row_key', 'id').cache()
        # original_df.show()

        '''将hash_index列展开至行，命名为h_v列'''
        hash_cols = ['hash_index_' + str(i) for i in self.bands_index]
        hv_exploded_df = original_df.select(
            'id',
            F.explode(F.array(hash_cols)).alias('h_v')
        )

        # hv_exploded_df.take(1)
        # self.logger.warn('将hash_index列展开至行，命名为h_v列')
        # hv_exploded_df.show()

        '''将h_v列中出现一次以上的hash value过滤出来，即找出所有有相同hash value的id'''
        hv_w = Window.partitionBy('h_v')
        hv_exploded_df = hv_exploded_df.select(
            'id',
            'h_v',
            F.count('id').over(hv_w).alias('cnt')
        ).filter(F.col('cnt') > 1).drop('cnt')

        # hv_exploded_df.take(1)
        # self.logger.warn('h_v列中出现一次以上的hash value过滤出来，即找出所有有相同hash value的id')
        # hv_exploded_df.show()

        '''找到所有有相同hash value的id的组合'''
        id_combination_df = hv_exploded_df.select(F.col('id').alias('src'), 'h_v')\
            .join(hv_exploded_df.select('h_v', F.col('id').alias('dst')), on=['h_v']). \
            filter(F.col('src') < F.col('dst'))

        # id_combination_df.take(1)
        # self.logger.warn('找到所有有相同hash value的id的组合')
        # id_combination_df.show()

        '''根据degree参数确定的阈值，过滤hash value的交集超过这个阈值的id对'''
        pair_w = Window.partitionBy('src', 'dst')
        candidate_df = id_combination_df.select(
            'src',
            'dst',
            F.count('src').over(pair_w).alias('cnt')
        ).dropDuplicates().filter(F.col('cnt') > degree).drop('cnt').cache()
        # candidate_df.take(1)
        # self.logger.warn('根据degree参数确定的阈值，过滤hash value的交集超过这个阈值的id对')
        # candidate_df.show()

        '''使用GraphFrame提供的对spark dataframe做disjoint set的方法找出所有id对所属的cluster'''
        self.spark.sparkContext.setCheckpointDir(f'hdfs:///tmp/{self.app_name}/graphframes_cps')
        gf = GraphFrame(original_df, candidate_df)

        clustered_df = gf.connectedComponents().cache()
        original_df.unpersist()
        candidate_df.unpersist()
        # clustered_df.take(1)
        # self.logger.warn('使用GraphFrame提供的对spark dataframe做disjoint set的方法找出所有id对所属的cluster')
        # clustered_df.show()

        '''将已有的c_id分配给相同组(component)的其它行'''
        component_w = Window.partitionBy('component').orderBy('component')
        clustered_df = clustered_df.select(
            *clustered_df.columns,
            F.last('c_id', True).over(component_w.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)).
                alias('new_c_id')
        )
        # clustered_df.take(1)
        # self.logger.warn('将已有的c_id分配给相同组(component)的其它行')
        # clustered_df.show()

        '''将没有c_id的行重新分配c_id'''
        cid_not_null_df = clustered_df.filter(F.col('new_c_id').isNotNull())
        # cid_not_null_df.take(1)
        # cid_not_null_df.show()
        cid_null_df = clustered_df.filter(F.col('new_c_id').isNull())
        # cid_null_df.take(1)
        # cid_null_df.show()

        ordered_w = Window.orderBy(F.desc('component'))
        prefix = self._basename_prefix()
        cid_assigned_df = cid_null_df.select(
            *cid_null_df.columns,
            F.concat_ws('_', F.lit(prefix), F.lit(F.dense_rank().over(ordered_w) + anchor - 1))
                .alias('assigned_cid')
        )
        # cid_assigned_df.take(1)
        # self.logger.warn('将没有c_id的行重新分配c_id')
        # cid_assigned_df.show()

        '''输出'''
        cid_assigned_df = cid_assigned_df.drop('new_c_id').withColumnRenamed('assigned_cid', 'new_c_id')
        final_df = cid_assigned_df.union(cid_not_null_df)
        final_df = final_df.drop('c_id').drop('component').withColumnRenamed('new_c_id', 'c_id').withColumnRenamed('id', 'row_key')
        # final_df.take(1)
        # self.logger.warn('输出')
        # final_df.show()

        return final_df

    def _clustering_new(self, dataframe, anchor, degree=1):

        if degree > self.bands:
            raise ValueError("Expecting degree less than %d, got %d" % (self.bands, degree))
        if not isinstance(degree, int):
            raise ValueError("Expecting degree is type of int, got %s" % type(degree))

        original_df = dataframe.withColumnRenamed('row_key', 'id')
        # original_df.show()

        '''将hash_index列展开至行，命名为h_v列'''
        hash_cols = ['hash_index_' + str(i) for i in self.bands_index]
        hv_exploded_df = original_df.select(
            'id',
            F.explode(F.array(hash_cols)).alias('h_v')
        )

       
        component_w = Window.partitionBy('h_v')
        candidate_df = hv_exploded_df.select(hv_exploded_df.id.alias("src"), F.first("id").over(component_w).alias("dst"))
        candidate_df = candidate_df.filter(candidate_df.src!=candidate_df.dst)


        '''使用GraphFrame提供的对spark dataframe做disjoint set的方法找出所有id对所属的cluster'''
        self.spark.sparkContext.setCheckpointDir(f'hdfs:///tmp/{self.app_name}/graphframes_cps')
        gf = GraphFrame(original_df, candidate_df)

        clustered_df = gf.connectedComponents().cache()
        original_df.unpersist()
        candidate_df.unpersist()
        # clustered_df.take(1)
        # self.logger.warn('使用GraphFrame提供的对spark dataframe做disjoint set的方法找出所有id对所属的cluster')
        # clustered_df.show()

        '''将已有的c_id分配给相同组(component)的其它行'''
        component_w = Window.partitionBy('component').orderBy('component')
        clustered_df = clustered_df.select(
            *clustered_df.columns,
            F.last('c_id', True).over(component_w.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)).
                alias('new_c_id')
        )
        # clustered_df.take(1)
        # self.logger.warn('将已有的c_id分配给相同组(component)的其它行')
        # clustered_df.show()

        '''将没有c_id的行重新分配c_id'''
        cid_not_null_df = clustered_df.filter(F.col('new_c_id').isNotNull())
        # cid_not_null_df.take(1)
        # cid_not_null_df.show()
        cid_null_df = clustered_df.filter(F.col('new_c_id').isNull())
        # cid_null_df.take(1)
        # cid_null_df.show()

        ordered_w = Window.orderBy(F.desc('component'))
        prefix = self._basename_prefix()
        cid_assigned_df = cid_null_df.select(
            *cid_null_df.columns,
            F.concat_ws('_', F.lit(prefix), F.lit(F.dense_rank().over(ordered_w) + anchor - 1))
                .alias('assigned_cid')
        )
        # cid_assigned_df.take(1)
        # self.logger.warn('将没有c_id的行重新分配c_id')
        # cid_assigned_df.show()

        '''输出'''
        cid_assigned_df = cid_assigned_df.drop('new_c_id').withColumnRenamed('assigned_cid', 'new_c_id')
        final_df = cid_assigned_df.union(cid_not_null_df)
        final_df = final_df.drop('c_id').drop('component').withColumnRenamed('new_c_id', 'c_id').withColumnRenamed('id', 'row_key')
        # final_df.take(1)
        # self.logger.warn('输出')
        # final_df.show()

        return final_df

    def _hash_udf(self):
        # TODO 优化Feature extraction
        window, permutations, bands, seed = self.window, self.permutations, self.bands, self.seed
        trie = Trie('data/tencent_word_freq.txt', pos=True)
        tokenizer = Tokenizer(trie)
        shingler = Shingler(window)

        def minhash_lsh(features):
            minhash = MinHash(features, permutations, bands, seed)
            return minhash.meta_hash

        return F.udf(lambda x: minhash_lsh(shingler(tokenizer(x))),
                     returnType=ArrayType(StringType()))

    def _basename_prefix(self):
        return self.app_name.upper() + '_P' + str(self.permutations) + 'B' + str(self.bands)


class RunCluster(MinHashCluster):
    __slots__ = ('source_table', 'base_table', 'incre_table', 'tmp_table', 'mysql_cmd')

    def __init__(self, app_name, mysql_config, log_level=None, **kwargs):
        super(RunCluster, self).__init__(app_name, mysql_config, log_level, **kwargs)
        for k, v in kwargs.items():
            setattr(self, k, v)

    def run_final(self, the_date, classifer_fun, anchor_sql):
        sql = self.source_table['query'].replace('t_date',the_date)
        df_in = self.read_table(sql, self.source_table['db_type'])
        dfi = self._hashing(df_in)
        dfi = dfi.select(*self.base_table['columns'],
                                F.lit(None).cast(StringType()).alias('c_id'),
                                F.lit(None).cast(ShortType()).alias('class_label'),
                                F.lit(None).cast(StringType()).alias('class_label_prob')
                                )

        domain_udf = F.udf(classifer_fun.multi_class, returnType=StringType())
        param = (dfi["msg"], dfi["app_name"], dfi["suspected_app_name"], dfi["hashcode"], dfi["abnormal_label"])
        dfi = dfi.select(*dfi.columns, domain_udf(*param).alias('industry'))
        sql = self.base_table['query'].replace('t_date',the_date)
        dfb = self.read_table(sql, self.base_table['db_type'])
        anchor = self.spark.sql(anchor_sql).collect()[0][0]
        print(f"*********anchor={anchor}")
        if not anchor: anchor = 0
        addudf = udf(add, returnType=LongType())
        df_out = dfi.join(dfb, concat(dfb.hash_index_0, dfb.hash_index_1,dfb.hash_index_2, dfb.hash_index_3) == concat(dfi.hash_index_0, dfi.hash_index_1, dfi.hash_index_2, dfi.hash_index_3),
                    'left').select(
                        dfi.row_key,
                        dfi.msg,
                        dfi.app_name,
                        dfi.suspected_app_name,
                        F.coalesce(dfi.c_id, dfb.c_id).alias("new_c_id"),
                        addudf(dfi.cnt, dfb.cnt).alias("cnt"), # 保证重刷某天数据时，该天数据不会被统计多次
                        dfi.industry,
                        dfi.class_label,
                        dfi.class_label_prob,
                        F.coalesce(dfi.hash_index_0, dfb.hash_index_0).alias("hash_index_0"),
                        F.coalesce(dfi.hash_index_1, dfb.hash_index_1).alias("hash_index_1"),
                        F.coalesce(dfi.hash_index_2, dfb.hash_index_2).alias("hash_index_2"),
                        F.coalesce(dfi.hash_index_3, dfb.hash_index_3).alias("hash_index_3"),
                        dfi.cnt.alias("date_cnt")
                    )
        not_null_df = df_out.filter(F.col('new_c_id').isNotNull())
        null_df = df_out.filter(F.col('new_c_id').isNull())

        ordered_w = Window.orderBy(F.desc('date_cnt'))
        prefix = self._basename_prefix()
        cid_assigned_df = null_df.select(
            *null_df.columns,
            F.concat_ws('_', F.lit(prefix), F.lit(F.row_number().over(ordered_w) + anchor))
                .alias('assigned_cid')
        )
        cid_assigned_df = cid_assigned_df.drop('new_c_id').withColumnRenamed('assigned_cid', 'new_c_id').select(*not_null_df.columns)
        final_df = not_null_df.union(cid_assigned_df)
        final_df = final_df.withColumnRenamed('new_c_id', 'c_id')
        print("########################")
        # final_df.show()
        self.write_table(final_df, self.base_table["table_name"], db_type=self.base_table['db_type'], partition=f"the_date='{the_date}'")



    def run_final_increment(self,the_date):
        sql = self.source_table['query'].replace('t_date',the_date)
        df_in = self.read_table(sql, self.source_table['db_type'])
        df_out = self._hashing(df_in)
        df_out = df_out.select(*self.incre_table['columns'],
                                F.lit(None).cast(ShortType()).alias('c_id'))    
        df_out.write.saveAsTable(self.incre_table['table_name'], mode='overwrite')

    def run_final_tmp(self,the_date, classifer_fun):
        domain_udf = F.udf(classifer_fun.multi_class, returnType=StringType())
        datename ='date-'+the_date
        dfb = self.read_table(self.base_table['query'], self.base_table['db_type'])
        dfi = self.read_table(self.incre_table['query'], self.incre_table['db_type'])
        param = (dfi["msg"], dfi["app_name"], dfi["suspected_app_name"], dfi["hashcode"], dfi["abnormal_label"])
        dfi = dfi.select(*dfi.columns, domain_udf(*param).alias('industry'))
        addudf = udf(add, returnType=LongType())
        base_column = [col for col in dfb.columns if re.search('date-', col) and col!= datename]
        df_out = dfb.join(dfi, concat(dfb.hash_index_0, dfb.hash_index_1,dfb.hash_index_2, dfb.hash_index_3) == concat(dfi.hash_index_0, dfi.hash_index_1, dfi.hash_index_2, dfi.hash_index_3),
                    'outer').select(
                        F.coalesce(dfi.row_key , dfb.row_key).alias('row_key'),
                        F.coalesce(dfi.msg, dfb.msg).alias("msg"),
                        F.coalesce(dfi.app_name, dfb.app_name).alias("app_name"),
                        F.coalesce(dfi.suspected_app_name, dfb.suspected_app_name).alias("suspected_app_name"),
                        F.coalesce(dfi.c_id, dfb.c_id).alias("c_id"),
                        addudf(dfi.cnt,*base_column).alias("cnt"), # 保证重刷某天数据时，该天数据不会被统计多次
                        F.coalesce(dfi.industry, dfb.industry).alias("industry"),
                        F.coalesce(dfi.class_label, dfb.class_label).alias("class_label"),
                        F.coalesce(dfi.hash_index_0, dfb.hash_index_0).alias("hash_index_0"),
                        F.coalesce(dfi.hash_index_1, dfb.hash_index_1).alias("hash_index_1"),
                        F.coalesce(dfi.hash_index_2, dfb.hash_index_2).alias("hash_index_2"),
                        F.coalesce(dfi.hash_index_3, dfb.hash_index_3).alias("hash_index_3"),
                        *base_column,
                        dfi.cnt.alias(datename),
                        F.coalesce(dfb.first_modified, lit(the_date)).alias("first_modified")
                    )
        df_out.write.saveAsTable(self.tmp_table['table_name'], mode='overwrite')


    def run_industry_increment(self, class_label, coverage=0.98, start=None, end=None,
                time_delta=1, rolling_window=7 ):
        weekname = f"week_{re.sub('-', '',start)}_{re.sub('-', '',end)}"
        dataframe =  self.read_table(self.source_table['query'], self.source_table['db_type'])
        HighFreq = HighFreqTemplate(dataframe, class_label, coverage, start, end, self.incre_table['columns'],
                 time_delta, rolling_window)
        dataframe = HighFreq()
        dataframe = dataframe.select(*self.incre_table['columns'],dataframe[weekname].alias('cnt'))
        dataframe.write.saveAsTable(self.incre_table['table_name'], mode='overwrite')


    def run_industry_tmp(self, anchor, start, end):
        # cntname= f'week-{start}-{end}'
        weekname = f"week_{re.sub('-', '',start)}_{re.sub('-', '',end)}"
        dfb = self.read_table(self.base_table['query'], self.base_table['db_type'])
        dfi = self.read_table(self.incre_table['query'], self.incre_table['db_type'])

        joinudf = udf(lambda x,y : x if x else y, returnType=StringType())
        addudf = udf(add, returnType=LongType())
        base_column = [col for col in dfb.columns if re.search('week', col) and col!=weekname]

        df_in = dfb.join(dfi, concat(dfb.hash_index_0, dfb.hash_index_1,dfb.hash_index_2, dfb.hash_index_3) == concat(dfi.hash_index_0, dfi.hash_index_1, dfi.hash_index_2, dfi.hash_index_3),
                    'outer').select(
                        F.coalesce(dfi.row_key , dfb.row_key).alias('row_key'),
                        F.coalesce(dfi.msg, dfb.msg).alias("msg"),
                        F.coalesce(dfi.app_name, dfb.app_name).alias("app_name"),
                        F.coalesce(dfi.suspected_app_name, dfb.suspected_app_name).alias("suspected_app_name"),
                        F.coalesce(dfi.c_id, dfb.c_id).alias("c_id"),
                        addudf(dfi.cnt,*base_column).alias("cnt"), # 保证重刷某天数据时，该天数据不会被统计多次
                        joinudf(dfi.class_label, dfb.class_label).alias("class_label"),
                        F.coalesce(dfi.industry, dfb.industry).alias("industry"),
                        F.coalesce(dfi.industry_label, dfb.industry_label).alias("industry_label"),
                        F.coalesce(dfi.industry_label_prob, dfb.industry_label_prob).alias("industry_label_prob"),
                        F.coalesce(dfi.hash_index_0, dfb.hash_index_0).alias("hash_index_0"),
                        F.coalesce(dfi.hash_index_1, dfb.hash_index_1).alias("hash_index_1"),
                        F.coalesce(dfi.hash_index_2, dfb.hash_index_2).alias("hash_index_2"),
                        F.coalesce(dfi.hash_index_3, dfb.hash_index_3).alias("hash_index_3"),
                        *base_column,
                        dfi.cnt.alias(weekname),
                        F.coalesce(dfb.first_modified, lit(weekname)).alias("first_modified")
                    )
        df_out = self._clustering_new(df_in, anchor)
        # df_out.write.format("orc").saveAsTable(self.tmp_table['table_name'], 'overwrite')
        df_out.write.saveAsTable(self.tmp_table['table_name'], mode='overwrite')


    def write_base(self):
        df = self.read_table(self.tmp_table['query'], self.tmp_table['db_type'])
        df.write.saveAsTable(self.base_table['table_name'], mode='overwrite', partitionBy=['first_modified'])


    def run_industry_increment_add(self, class_label, classifer_fun, coverage=0.98, start=None, end=None,
                time_delta=1, rolling_window=7, ):
        weekname = f"week_{re.sub('-', '', start)}_{re.sub('-', '', end)}"
        
        dfi=  self.read_table(self.source_table['query'], self.source_table['db_type'])

        domain_udf = F.udf(classifer_fun.multi_class, returnType=StringType())
        rawcols = dfi.columns
        cols = dfi.columns
        cols.remove("industry")
        #param = (dfi["msg"], dfi["app_name"], dfi["suspected_app_name"], "hashcode", "abnormal_label")
        #dataframe = dfi.select(*cols,F.lit('0').alias('hashcode'),F.lit('正常文本').alias('abnormal_label')).select(*cols, domain_udf(*param).alias('industry')).select(*rawcols)
        param = (dfi["msg"], dfi["app_name"], dfi["suspected_app_name"], F.lit("0"), F.lit("正常文本"))
        dataframe = dfi.select(*cols, domain_udf(*param).alias("industry"))
        HighFreq = HighFreqTemplate(dataframe, class_label, coverage, start, end,
                 time_delta, rolling_window)
        dataframe = HighFreq()
        dataframe = dataframe.select(*self.incre_table['columns'],dataframe[weekname].alias('cnt'))
        dataframe.write.saveAsTable(self.incre_table['table_name'], mode='overwrite')

