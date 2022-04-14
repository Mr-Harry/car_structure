from string import Template
from datetime import datetime as dt
from datetime import timedelta
import sys
import subprocess
import os
import re
import argparse
import pandas as pd
import json

ABSPATH = os.path.dirname(os.path.abspath(__file__))
ABSPATH_F = os.path.dirname(ABSPATH)

if __name__ == "__main__":
    # 定义参数
    parser = argparse.ArgumentParser(description="目标数据获取对外统一接口")
    parser.add_argument('--config_name', default=None,
                        dest='config_name', type=str, help='配置参数信息')
    parser.add_argument('--the_date', default=None,
                        dest='the_date', type=str, help='需要处理的the_date分区')
    parser.add_argument('--file_no', default=None,
                        dest='file_no', type=str, help='需要处理的file_no分区')
    parser.add_argument('--config_type', default='master',
                        dest='config_type', type=str, help='配置文件类型')
    args = parser.parse_args()
    print('目标数据获取解析接受到如下参数 config_name:{0} the_date:{1} file_no:{2} config_type:{3}'.format(
        args.config_name, args.the_date, args.file_no, args.config_type))
    config_file_path = os.path.join(
        ABSPATH_F, 'config', args.config_name, 'config_'+args.config_type+'.json')
    dict_list_file_path = os.path.join(
        ABSPATH_F, 'config', args.config_name, 'dict_list_file.json')
    config_dict = json.load(open(config_file_path, 'r', encoding='utf-8'))
    domain_table = config_dict['domain_table']
    source_table = config_dict['source_table']
    single_partition = 'True' if config_dict.get('single_partition',False) else 'False'
    the_date = args.the_date
    file_no = args.file_no
    class_name =  args.config_name + '_'+args.config_type
    # 调用接口
    result = subprocess.call(["sh", os.path.join(ABSPATH, './data_extractor.sh'), '--the_date', the_date, '--file_no', file_no,
                              '--source_table', config_dict.get(
                                  'source_table'),
                              '--domain_table', config_dict.get(
                                  'domain_table'),
                              '--class_name',class_name,
                              '--config_name', args.config_name,
                              '--dict_list_file', dict_list_file_path,
                              '--single_partition',single_partition
                              ])
    # 返回结果
    if result != 0:
        raise ValueError("任务执行失败")
