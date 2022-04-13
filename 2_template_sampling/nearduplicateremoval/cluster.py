import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from connector import BaseSparkConnector
from utils import Trie, Tokenizer, Shingler
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
        
        '''如果不是对增量数据进行聚类，需要新建一列初始值为空的c_id列'''
        if 'c_id' not in dataframe.columns:
            dataframe = dataframe.withColumn('c_id', F.lit(None).cast(StringType()))        

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

    def _hash_udf(self):
        # TODO 优化Feature extraction
        window, permutations, bands, seed = self.window, self.permutations, self.bands, self.seed
        trie = Trie('token_dicts/tencent_word_freq.txt', pos=True)
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
    __slots__ = ('source_table', 'target_table')

    def __init__(self, app_name, mysql_config, log_level=None, **kwargs):
        super(RunCluster, self).__init__(app_name, mysql_config, log_level, **kwargs)
        for k, v in kwargs.items():
            setattr(self, k, v)

    def run(self):
        query = self.source_table['query']
        df_orig = self.read_table(query, self.source_table['db_type'])
        df_hashed = self._hashing(df_orig)
        df_clustered = self._clustering(df_hashed, 1)
        out_columns = self.target_table['columns']
        df_out = df_clustered.select(*out_columns)
        self.write_table(df_out, self.target_table['table_name'], self.target_table['db_type'])




    

