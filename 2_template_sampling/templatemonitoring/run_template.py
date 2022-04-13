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
    sys_args = parser.parse_args()
    sys_config = yaml.load(open(sys_args.config_file, 'r', encoding='utf-8').read(), Loader=yaml.SafeLoader)

    for conf in ['source_table', 'base_tabl', 'mysql_cmd']:
        sys_config['minhash'][conf] = sys_config[conf]

    domain_funs = Classifer(sys_config['class_dict'])
    anchor_sql = sys_config['anchor_sql']
    the_date = sys_args.the_date
    cluster = RunCluster(app_name=sys_config['app_name'],
                         mysql_config=sys_config['mysql_config'],
                         log_level=sys_config['log_level'],
                         **sys_config['minhash'])

    cluster.run_final(the_date, domain_funs, anchor_sql)

    cluster.stop()