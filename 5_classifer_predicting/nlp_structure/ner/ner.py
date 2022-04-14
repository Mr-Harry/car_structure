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
import time

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
    parser = argparse.ArgumentParser(description="实体模块对外统一接口")
    parser.add_argument('--config_name', default=None,
                        dest='config_name', type=str, help='配置参数信息')
    parser.add_argument('--the_date', default=None,
                        dest='the_date', type=str, help='需要处理的the_date分区')
    parser.add_argument('--file_no', default=None,
                        dest='file_no', type=str, help='需要处理的file_no分区')
    parser.add_argument('--config_type', default='master',
                        dest='config_type', type=str, help='配置文件类型')
    parser.add_argument('--ner_dict',type=str,default='all',help='自定义需要跑的NER类目')
    args = parser.parse_args()
    print('实体提取获取解析接受到如下参数 config_name:{0} the_date:{1} file_no:{2} config_type:{3}'.format(
        args.config_name, args.the_date, args.file_no,  args.config_type))
    config_file_path = os.path.join(
        ABSPATH_F, 'config', args.config_name, 'config_'+args.config_type+'.json')
    config_dict = json.load(open(config_file_path, 'r', encoding='utf-8'))
    run_py_path = os.path.join(ABSPATH_F, './run.py')
    ner_sh_file_path = os.path.join(
        ABSPATH_F, 'config', args.config_name, 'ner_run.sh')
    col_file_path = os.path.join(
        ABSPATH_F, 'config', args.config_name, 'col_config.json')
    ner_config_file = os.path.join(
        ABSPATH_F, 'tmp', args.config_name + '_ner_%.5f' % (time.time()) + '.json')

    # 解析配置信息
    the_date = args.the_date
    file_no = args.file_no
    class_name =  args.config_name + '_'+args.config_type
    hdfs_base_path = '{0}/{1}_{2}/'.format(config_dict.get(
        'hdfs_base_path'), args.the_date, args.file_no)
    nlp_table_dict = config_dict.get('nlp_table_dict', {})
    need_ner_dict = config_dict.get('need_ner_dict', {})
    ner_model_dict = config_dict.get('ner_model_dict', {})
    single_partition = 'True' if config_dict.get('single_partition',False) else 'False'
    # step0 进行hdfs目录的清空和删除工作
    run_cmd('hdfs dfs -rm -r ' + hdfs_base_path)
    run_cmd('hdfs dfs -mkdir ' + hdfs_base_path)
    ner_dict_list = args.ner_dict.split(',') if args.ner_dict != 'all' else []
    # step1 模型训练前的数据分发工作，对需不需要ner处理的数据分别进行不同的处理
    result = subprocess.call(["sh", os.path.join(ABSPATH, './ner_data_distributor.sh'),
                              '--config', config_file_path,
                              '--the_date', the_date, '--file_no', file_no,
                              '--class_name', class_name,
                              '--single_partition', single_partition,'--ner_dict',args.ner_dict])
    if result != 0:
        raise ValueError("数据分发任务执行失败")
    # step2 使用模型进行数据的预测工作
    # step2.1 配置文件更新
    model_config = []
    for class_label in nlp_table_dict.keys():
        if (ner_dict_list and class_label in ner_dict_list) or not ner_dict_list:
            # 先检查当天该类目是否有需要预测的数据
            class_label_str = class_label.replace(
                ',', '__') if ',' in class_label else class_label
            res = run_cmd('hdfs dfs -ls ' + hdfs_base_path +
                        'ner_result/' + class_label_str)
            # 如果需要进行实体抽取并且当前有需要预测的数据（没有相应文件夹就是有需要预测的数据）
            if need_ner_dict.get(class_label) == '1' and res.returncode != 0:
                model_config_dict = {}
                model_config_dict["out_path"] = hdfs_base_path + \
                    'ner_result/' + class_label_str
                model_config_dict["root_path"] = hdfs_base_path + 'to_ner'
                model_config_dict["files"] = [class_label_str]
                model_config_dict["model_name"] = ner_model_dict.get(class_label)
                model_config_dict['hdfs'] = True
                model_config.append(model_config_dict)
    with open(ner_config_file, 'w') as f:
        json.dump(model_config, f, ensure_ascii=False)
    # step2.2 模型预测工作
    class_name_ = class_name.replace('_', '-')
    if model_config:
        result = subprocess.call(["python3", '-u', run_py_path, class_name_ + '-ner', '2',
                                  ner_config_file, ner_sh_file_path, 'java-nlp-112701'])
        if result != 0:
            raise ValueError("模型实体抽取任务执行失败")
    # step2.3 上传数据
    result = subprocess.call(["sh", os.path.join(ABSPATH, './ner_data_uploader.sh'),
                              '--config', config_file_path,
                              '--the_date', the_date, '--file_no', file_no,
                              '--col_config', col_file_path,
                              '--class_name', class_name,
                              '--single_partition', single_partition,'--ner_dict',args.ner_dict])
    if result != 0:
        run_cmd('rm -r ' + ner_config_file, raise_err=False)
        raise ValueError("数据上传任务执行失败")
    run_cmd('rm -r ' + ner_config_file, raise_err=False)
    # step3 删除hdfs目录数据
    run_cmd('hdfs dfs -rm -r ' + hdfs_base_path)
