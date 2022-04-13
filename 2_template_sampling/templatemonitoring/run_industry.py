import argparse
import yaml
from cluster import RunCluster
import datetime 


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='near duplicates')
    parser.add_argument('--config_file',
                        type=str,
                        default='./incre_config.yaml',
                        help='Path of config file, default is ./config.yaml')
    parser.add_argument('--start',
                        type=str,
                        default='2021-01-01')
    parser.add_argument('--end',
                        type=str,
                        default='2021-01-01')
    parser.add_argument('--step',
                        type=str,
                        default='0')

    sys_args = parser.parse_args()
    sys_config = yaml.load(open(sys_args.config_file, 'r', encoding='utf-8').read(), Loader=yaml.SafeLoader)

    for conf in ['source_table', 'base_table', 'incre_table', 'tmp_table', 'mysql_cmd']:
        sys_config['minhash'][conf] = sys_config[conf]
   
    monday = sys_args.start
    sunday = sys_args.end

    anchor_sql = sys_config['anchor_sql']
    class_label = sys_config['class_label']

    cluster = RunCluster(app_name=sys_config['app_name'],
                         mysql_config=sys_config['mysql_config'],
                         log_level=sys_config['log_level'],
                         **sys_config['minhash'])

    if sys_args.step=='0':
        cluster.run_industry_increment(class_label, start=monday, end=sunday)
        print(f"{monday}至{sunday}行业{class_label}增量数据加载完成")
    elif sys_args.step=='1':
        anchor = cluster.spark.sql(anchor_sql).collect()[0][0]
        anchor = anchor + 1 if anchor else 1 
        cluster.run_industry_tmp(anchor, monday, sunday)   
        print(f"{monday}至{sunday}行业{class_label}增量数据融合成功")
    elif sys_args.step=='2':
        cluster.write_base()
        anchor = cluster.spark.sql(anchor_sql).collect()[0][0]
        print(f"{monday}至{sunday}行业{class_label}模板已更新，当前模板最大为{anchor}")

    cluster.stop()