import argparse
import yaml
from cluster import RunCluster

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='near duplicates')
    parser.add_argument('--config_file',
                        type=str,
                        help='Path of config file, config example is ./config/example.yaml')
    sys_args = parser.parse_args()
    sys_config = yaml.load(open(sys_args.config_file, 'r', encoding='utf-8').read(), Loader=yaml.SafeLoader)

    cluster = RunCluster(app_name=sys_config['app_name'],
                         mysql_config=sys_config['mysql_config'],
                         log_level=sys_config['log_level'],
                         **sys_config['minhash'])
    cluster.run()
    cluster.stop()


