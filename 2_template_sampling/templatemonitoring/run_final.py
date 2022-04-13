import argparse
import yaml
from cluster import RunCluster
import datetime
from classifer import Classifer


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='near duplicates')
    parser.add_argument('--config_file',
                        type=str,
                        default='./incre_config.yaml',
                        help='Path of config file, default is ./config.yaml')
    parser.add_argument('--the_date',
                        type=str,
                        default='2021-01-01')
    parser.add_argument('--step',
                        type=str,
                        default="increment")
    sys_args = parser.parse_args()
    sys_config = yaml.load(open(sys_args.config_file, 'r', encoding='utf-8').read(), Loader=yaml.SafeLoader)

    for conf in ['source_table', 'base_table', 'incre_table', 'tmp_table', 'mysql_cmd']:
        sys_config['minhash'][conf] = sys_config[conf]

    domain_funs = Classifer(sys_config['class_dict'])
    the_date = sys_args.the_date
    cluster = RunCluster(app_name=sys_config['app_name'],
                         mysql_config=sys_config['mysql_config'],
                         log_level=sys_config['log_level'],
                         **sys_config['minhash'])
    if sys_args.step == '0':
        cluster.run_final_increment(the_date)
        print(f"{the_date}增量数据加载完成")
    elif sys_args.step == "1":
        cluster.run_final_tmp(the_date, domain_funs)
        print(f"{the_date}增量数据融合成功")
    elif sys_args.step == "2":
        cluster.write_base()
        print(f"{the_date}模板已更新")
    else:
        print("ERROR STEP")

    cluster.stop()