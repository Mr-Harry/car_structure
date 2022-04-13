import datetime
import re
import pyspark.sql.functions as F
from pyspark.sql.window import Window


class HighFreqTemplate:
    def __init__(self, dataframe, class_label=None, coverage=0.98, start=None, end=None,
                 time_delta=1, rolling_window=7):
        self.start = datetime.datetime.strptime(start, '%Y-%m-%d') if start else None
        self.end = datetime.datetime.strptime(end, '%Y-%m-%d') if end else None
        self.time_delta = datetime.timedelta(days=time_delta)
        self.rolling_window = rolling_window
        self.time_interval = self._get_time_range(dataframe.columns)
        self.interval_name = 'week-' + self.start.date().__str__() + '-' + self.end.date().__str__()
        self.high_freq_tmplt = self._get_high_freq_tmplt(dataframe, class_label, coverage)
        self.size = self.high_freq_tmplt.count()

    def __call__(self):
        """
        返回高频模版的Dataframe
        :return: Dataframe
        """
        return self.high_freq_tmplt

    def _get_time_range(self, columns):
        time_interval = []
        if self.start is not None and self.end is not None:
            date = self.start
            while date <= self.end:
                time_interval.append(date.date().__str__())
                date += self.time_delta

        elif self.start is None and self.end is None:
            pattern = r'week-(\d{4}-\d{2}-\d{2})-(\d{4}-\d{2}-\d{2})'
            week_cols = [x for x in columns if re.match(pattern, x)]
            latest_end = re.search(pattern, max(week_cols)).group(2)
            latest_end = datetime.datetime.strptime(latest_end, '%Y-%m-%d')
            current = self.start = latest_end + self.time_delta
            self.end = latest_end + datetime.timedelta(days=self.rolling_window)
            columns = set(columns)
            while current <= self.end:
                if 'day-' + current.date().__str__() not in columns:
                    raise ValueError(f'{self.start.date()}至{self.end.date()}数据有缺失，请检查')
                time_interval.append(current.date().__str__())
                current += self.time_delta

        else:
            raise ValueError('请检查起始日期和结束日期的设置')

        return time_interval

    def _get_high_freq_tmplt(self, dataframe, class_label, coverage):

        if class_label is not None:
            '''筛选出指定行业的数据'''
            # dataframe = dataframe.filter(F.array_contains(F.col('industry'), class_label))
            dataframe = dataframe.filter(F.col('industry').contains(class_label))

        '''计算出时间窗口内的统计值'''
        date_cols = ['day-' + x for x in self.time_interval]
        dataframe = dataframe.select(
            *dataframe.columns,
            sum([F.col(col) for col in date_cols]).alias(self.interval_name)
        )

        if coverage is not None:
            dataframe = dataframe.select(
                *dataframe.columns,
                (F.col(self.interval_name)/F.sum(self.interval_name).over(Window.partitionBy())).alias('percent')
            )
            #dataframe.show()
            threshold = dataframe.approxQuantile('percent', probabilities=[1-coverage], relativeError=0.01)[0]
            dataframe = dataframe.filter(F.col('percent') > threshold).drop('percent')
        return dataframe


class Monitor:
    def __init__(self, dataframe):
        self.stats = []  # Stats list
        self.dataframe = dataframe
        self.week_cols = [x for x in dataframe.columns if re.match(r'week_(\d{8})_(\d{8})', x)]
        self.week_cols = sorted(self.week_cols)
        self.cur_week, self.prev_week = self.week_cols[-1], self.week_cols[-2]

    def quantity_variance(self, partition=None, app_names=None):
        """
        高频模版数量的变化
        :param partition: (None, 'industry_label', 'app_name') 为三种可支持的分区方式，用以不同维度的数量变化
        :param app_names: 如果以app_name为分区，需要传入监控的app_name列表，格式为 <class 'list'>
        :return: 结果表的dataframe
        """
        if partition not in [None, 'industry_label', 'app_name']:
            raise ValueError("请检查高频模版数量变化的分区设置，必须在 (None, 'industry_label', 'app_name') 之中选择")

        if partition == 'app_name' and (not isinstance(app_names, list) or not app_names):
            raise ValueError("请检查app_name的列表是否正确设置")

        w_cid = Window.partitionBy('c_id')
        # agg_df = self.dataframe.groupBy('c_id').agg(*[F.sum(x).alias(x) for x in self.week_cols])
        cols = list(set(self.dataframe.columns) - set(self.week_cols))
        agg_df = self.dataframe.select(
            *cols,
            F.sum(self.prev_week).over(w_cid.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
                .alias(self.prev_week),
            F.sum(self.cur_week).over(w_cid.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
                .alias(self.cur_week),
            F.rank().over(w_cid.orderBy('msg', F.length('msg').desc())).alias('rank')
        ).filter(F.col('rank') == 1).drop('rank')

        #agg_df.show()

        if partition is None:
            qty_df = agg_df.groupBy().agg(
                F.count(F.when(F.col(self.cur_week).isNotNull(), True)).alias('cur_week_cnt'),
                F.count(F.when(F.col(self.cur_week).isNotNull() & F.col(self.prev_week).isNull(), True)).alias(
                    'increment'),
                F.count(F.when(F.col(self.cur_week).isNull() & F.col(self.prev_week).isNotNull(), True)).alias(
                    'decrement')
            )

            res_df = qty_df.select(
                F.lit(self.cur_week).alias('week'),
                'cur_week_cnt',
                'increment',
                'decrement'
            )

        else:
            if partition == 'app_name':
                agg_df = agg_df.filter(F.col('app_name').isin(app_names) | F.col('suspected_app_name').isin(app_names))
            qty_df = agg_df.groupBy(partition).agg(
                F.count(F.when(F.col(self.cur_week).isNotNull(), True)).alias('cur_week_cnt'),
                F.count(F.when(F.col(self.cur_week).isNotNull() & F.col(self.prev_week).isNull(), True)).alias(
                    'increment'),
                F.count(F.when(F.col(self.cur_week).isNull() & F.col(self.prev_week).isNotNull(), True)).alias(
                    'decrement')
            )

            res_df = qty_df.select(
                F.lit(self.cur_week).alias('week'),
                partition,
                'cur_week_cnt',
                'increment',
                'decrement'
            )
        return res_df
        #res_df.show()
















    


