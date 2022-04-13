import datetime
from functools import partial
from logging import raiseExceptions
import re
import pyspark.sql.functions as F
from connector import BaseSparkConnector
import argparse
import yaml
from monitor import Monitor

class Data_Intelligence(BaseSparkConnector):

    def __init__(self, app_name, mysql_config=None,log_level=None, **kwarg):
        super().__init__(app_name, mysql_config, log_level)
        for k,v in kwarg.items():
            setattr(self, k, v)

    def run_final(self, source_table, dest_table):
        # T1 获取final表数据
        df = self.read_table(source_table["query"], source_table["db_type"])


        # T2 统计相应数据：1每天的数据量（全类别、各类别），模板量（分类别），高频模板量（分类别），
        # T2.1 把日期数据，行转列
        cols = [_ for _ in df.columns if 'date' in _]
        nocols = [_ for _ in df.columns if 'date' not in _]
        
        stack_str = ','.join(map(lambda x: f"'{x[5:]}', `{x}`", cols))
        df = df.selectExpr(*nocols, f"stack({len(cols)}, {stack_str}) as (date, dcnt)")
        df = df.filter(df.dcnt>0)

        # T2.2 按行业拆数据
        df = df.select(*df.columns, F.explode(F.split(df.industry,',')).alias('single_industry'))
        
        res = df.groupBy([df.date, df.single_industry]).agg(F.first(df.class_label).alias('class_label'),
                                                            F.sum(df.dcnt).alias('cnt'), 
                                                            F.count(df.class_label).alias('templet_num'),
                                                            F.sum(F.when(df.cnt>100,1).otherwise(0)).alias('high_templet_num'))
        data = res.collect()
        print(data)
        # T3 上传结果到MySQL
        self.write_table(res, dest_table["table_name"], dest_table["db_type"]) 
    
    def run_industry(self, source_table, dest_table):
        df = self.read_table(source_table["query"], source_table["db_type"])
        monitor = Monitor(df)
        res_df = monitor.quantity_variance()
        self.write_table(res_df, dest_table["table_name"], dest_table["db_type"])


if __name__=="__main__":
    parser = argparse.ArgumentParser('templet_BI')
    parser.add_argument("--config_file", type=str, default='bi_config/final.yaml')
    parser.add_argument("--task_type", type=str, default='final',choices=['final', 'industry'])
    args=parser.parse_args()
    config_dict = yaml.load(open(args.config_file, 'r', encoding='utf-8').read(), Loader=yaml.SafeLoader)

    mysql_config = config_dict["mysql_config"]
    source_table = config_dict["source_table"]
    dest_table = config_dict["dest_table"]

    DBI = Data_Intelligence(app_name="templet_BI", mysql_config=mysql_config)
    if args.task_type=='final':
        DBI.run_final(
            source_table, dest_table
        )
    elif args.task_type=='industry':
        DBI.run_industry(
            source_table, dest_table
        )
    else:
        raise ValueError("任务参数上传错误")

    

    

    