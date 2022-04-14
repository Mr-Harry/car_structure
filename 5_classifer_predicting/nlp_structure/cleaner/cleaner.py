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

# 工具函数


def run_cmd(command, print_log=True, raise_err=False):
    """ 执行命令行命令

        Args:
            command: 必填参数，需要执行的命令;  str
            pring_log: 选填参数，是否打印返回结果; bool
            raise_err: 选填参数，是否在cmd跑失败的时候抛出异常; bool

        Returns:
            执行命令结果; subprocess.CompletedProcess类
    """
    res = subprocess.run(command, shell=True)
    if print_log:
        print(res)
    if raise_err and res.returncode != 0:
        raise ValueError("shell命令执行失败")
    return res


if __name__ == "__main__":
    # 定义参数
    parser = argparse.ArgumentParser(description="清洗模块对外统一接口")
    parser.add_argument('--config_name', default=None,
                        dest='config_name', type=str, help='配置参数信息')
    parser.add_argument('--the_date', default=None,
                        dest='the_date', type=str, help='需要处理的the_date分区')
    parser.add_argument('--file_no', default=None,
                        dest='file_no', type=str, help='需要处理的file_no分区')
    parser.add_argument('--config_type', default='master',
                        dest='config_type', type=str, help='配置文件类型')
    parser.add_argument('--test_class_name', default=None,
                    dest='test_class_name', type=str, help='单独测试某个细类目')
    args = parser.parse_args()
    print('dwb清洗解析接受到如下参数 config_name:{0}  the_date:{1}  file_no:{2}  config_type:{3}  test_class_name:{4}\n'.format(
        args.config_name, args.the_date, args.file_no,  args.config_type,args.test_class_name))
    config_file_path = os.path.join(
        ABSPATH_F, 'config', args.config_name, 'config_'+args.config_type+'.json')
    col_file_path = os.path.join(
        ABSPATH_F, 'config', args.config_name, 'col_config.json')
    dict_list_file_path = os.path.join(
        ABSPATH_F, 'config', args.config_name, 'dict_list_file.json')
    class_name =  args.config_name + '_'+args.config_type
    # 把数据写入到表中
    test_class_name = ['--test_class_name',args.test_class_name] if args.test_class_name is not None else ['','']
    result = subprocess.call(["sh", os.path.join(ABSPATH, './data_cleaner.sh'),
                              '--the_date', args.the_date, '--file_no', args.file_no,
                              '--config', config_file_path,
                              '--config_name', args.config_name,
                              '--col_config', col_file_path,
                              '--class_name', class_name,
                              '--dict_list_file', dict_list_file_path,
                              test_class_name[0],test_class_name[1]
                              ])
    if result != 0:
        raise ValueError("{0} {1}数据清洗任务执行失败".format(
            args.the_date, args.file_no))
