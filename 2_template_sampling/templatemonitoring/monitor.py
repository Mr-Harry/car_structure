import datetime
import re
import pyspark.sql.functions as F
from pyspark.sql.window import Window


class HighFreqTemplate:
    def __init__(self, dataframe, class_label=None, coverage=0.98, start=None, end=None, cols=None,
                 time_delta=1, rolling_window=7):
        self.start = datetime.datetime.strptime(start, '%Y-%m-%d') if start else None
        self.end = datetime.datetime.strptime(end, '%Y-%m-%d') if end else None
        self.time_delta = datetime.timedelta(days=time_delta)
        self.rolling_window = rolling_window
        self.cols=cols
        self.time_interval = self._get_time_range(dataframe.columns)
        self.interval_name = 'week_' + self.start.strftime('%Y%m%d') + '_' + self.end.strftime('%Y%m%d')
        self.high_freq_tmplt = self._get_high_freq_tmplt(dataframe, class_label, coverage)
        # self.size = self.high_freq_tmplt.count()

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
            pattern = r'week_(\d{8})_(\d{8})'
            week_cols = [x for x in columns if re.match(pattern, x)]
            latest_end = re.search(pattern, max(week_cols)).group(2)
            latest_end = datetime.datetime.strptime(latest_end, '%Y-%m-%d')
            current = self.start = latest_end + self.time_delta
            self.end = latest_end + datetime.timedelta(days=self.rolling_window)
            columns = set(columns)
            while current <= self.end:
                if 'date-' + current.date().__str__() not in columns:
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
        date_cols = ['date-' + x for x in self.time_interval]
        dataframe = dataframe.select(
            *self.cols,
            sum([F.when(F.col(col).isNotNull(), F.col(col)).otherwise(0) for col in date_cols]).alias(self.interval_name)
        )

        if coverage is not None:
            dataframe = dataframe.select(
                *dataframe.columns,
                F.col(self.interval_name).alias('percent')
            )
            #dataframe.show()
            threshold = dataframe.approxQuantile('percent', probabilities=[1-coverage], relativeError=0.01)[0]
            dataframe = dataframe.filter(F.col('percent') > threshold).drop('percent')
        return dataframe


class Monitor:
    def __init__(self, dataframe):
        self.stats = []  # Stats list
        self.week_cols = [x for x in dataframe.columns
                          if re.match(r'week-(\d{4}-\d{2}-\d{2})-(\d{4}-\d{2}-\d{2})', x)]
    
    def add_stat_column(self):
        raise NotImplementedError

    


