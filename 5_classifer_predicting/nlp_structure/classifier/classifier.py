from string import Template
from datetime import datetime as dt
from datetime import timedelta
import sys
import time
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
    parser = argparse.ArgumentParser(description="分类模块对外统一接口")
    parser.add_argument('--config_name', default=None,
                        dest='config_name', type=str, help='配置参数信息')
    parser.add_argument('--the_date', default=None,
                        dest='the_date', type=str, help='需要处理的the_date分区')
    parser.add_argument('--file_no', default=None,
                        dest='file_no', type=str, help='需要处理的file_no分区')
    parser.add_argument('--config_type', default='master',
                        dest='config_type', type=str, help='配置文件类型')
    args = parser.parse_args()
    print('数据分类获取解析接受到如下参数 config_name:{0} the_date:{1} file_no:{2} config_type:{3}'.format(
        args.config_name, args.the_date, args.file_no,  args.config_type))

    config_file_path = os.path.join(
        ABSPATH_F, 'config', args.config_name, 'config_'+args.config_type+'.json')

    config_file = os.path.join(
        ABSPATH_F, 'tmp', args.config_name + '_class_%.5f' % (time.time()) + '.json')
    dict_list_file_path = os.path.join(
        ABSPATH_F, 'config', args.config_name, 'dict_list_file.json')
    class_sh_file_path = os.path.join(
        ABSPATH_F, 'config', args.config_name, 'class_run.sh')
    run_py_path = os.path.join(ABSPATH_F, './run.py')
    config_dict = json.load(open(config_file_path, 'r', encoding='utf-8'))

    domain_table = config_dict['domain_table']
    classified_table = config_dict['classified_table']
    single_partition = 'True' if config_dict.get('single_partition',False) else 'False'

    the_date = args.the_date
    file_no = args.file_no
    class_name =  args.config_name + '_'+args.config_type
    hdfs_base_path = '{0}/{1}_{2}/'.format(config_dict.get(
        'hdfs_base_path'), args.the_date, args.file_no)
    # step0 进行hdfs目录的清空和删除工作
    run_cmd('hdfs dfs -rm -r ' + hdfs_base_path)
    run_cmd('hdfs dfs -mkdir ' + hdfs_base_path)

    not_class = config_dict.get('not_class', False)
    result = -1

    if not_class:
        result = subprocess.call(["sh", os.path.join(ABSPATH, './uploader.sh'), '--the_date', the_date, '--file_no', file_no,
                                  '--domain_table', domain_table,
                                  '--classified_table', classified_table,
                                  '--class_name', class_name,'--single_partition',single_partition])
    else:
        result = subprocess.call(["sh", os.path.join(ABSPATH, './pre_classifier.sh'), '--the_date', the_date, '--file_no', file_no,
                                  '--domain_table', domain_table,
                                  '--hdfs_base_path', hdfs_base_path,
                                  '--class_name', class_name,
                                  '--dict_list_file', dict_list_file_path,'--single_partition',single_partition])

        # step1 进行预分类工作

        if result != 0:
            raise ValueError("预分类任务执行失败")
        # step2 进行模型分类工作
        # step2.0 判断下hdfs上是否有相应的目录
        res = run_cmd('hdfs dfs -ls ' + hdfs_base_path + '/model_classified')
        # 如果目录不存在 那么开始跑模型
        if res.returncode != 0:
            # step2.1 进行配置文件的更新
            model_config_dict = {}
            model_config_dict["out_path"] = hdfs_base_path + \
                '/model_classified'
            model_config_dict["root_path"] = hdfs_base_path + '/'
            model_config_dict["files"] = ['unclassified']
            model_config_dict["model_name"] = config_dict.get('classify_model')
            model_config_dict['hdfs'] = True
            model_config = [model_config_dict]
            # 写回原文件

            with open(config_file, 'w') as f:
                json.dump(model_config, f, ensure_ascii=False)
            # step2.2进行模型的预测工作
            class_name = args.config_name.replace('_', '-')
            result = subprocess.call(["python3", '-u', run_py_path, class_name + '-classifier',
                                      '2', config_file, class_sh_file_path, 'java-nlp-112701'])
            if result != 0:
                run_cmd('rm -r ' + config_file, raise_err=False)
                raise ValueError("模型分类任务执行失败")
            run_cmd('rm -r ' + config_file, raise_err=False)
        else:
            pass

        # step2.3进行数据上传工作
        result = subprocess.call(["sh", os.path.join(ABSPATH, './data_uploader.sh'),
                                  '--the_date', the_date, '--file_no', file_no,
                                  '--classified_table', classified_table,
                                  '--hdfs_base_path', hdfs_base_path,
                                  '--class_name', class_name,'--single_partition',single_partition])
    if result != 0:
        raise ValueError("分类成果上传执行失败")
    # step3 删除hdfs目录数据

    run_cmd('hdfs dfs -rm -r ' + hdfs_base_path)
