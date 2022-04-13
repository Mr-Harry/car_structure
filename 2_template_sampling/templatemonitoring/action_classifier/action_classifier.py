# coding: utf-8
import argparse
import subprocess
import os
import re
import time
import json
import yaml
from string import Template


ABSPATH = os.path.dirname(os.path.abspath(__file__))
ABSPATH_F = os.path.dirname(ABSPATH)


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


if __name__=="__main__":
    parser = argparse.ArgumentParser('模板行业_行为分类')
    parser.add_argument("--config_file",type=str, default='./industry_config.yaml')

    args=parser.parse_args()
    config_dict = yaml.load(open(os.path.join(ABSPATH_F, args.config_file), 'r', encoding='utf-8').read(), Loader=yaml.SafeLoader)
    config_name = config_dict['config_name']
    config_file = os.path.join(
        ABSPATH, config_name + '_class_%.5f' % (time.time()) + '.json')
    dict_list_file_path = os.path.join(
        ABSPATH_F, 'config', config_name, 'dict_list_file.json')
    class_sh_file_path = os.path.join(
        ABSPATH_F, 'config', config_name, 'class_run.sh')
    run_py_path = os.path.join(ABSPATH, './run.py')

    domain_table = config_dict['domain_table']
    classified_table = config_dict['classified_table']

    # modified_date = f"week_{re.sub('-','',args.start)}_{re.sub('-','',args.end)}"
    class_name =  config_name
    hdfs_base_path = '{0}/'.format(config_dict.get(
        'hdfs_base_path'))
    
    # T1 数据到hdfs
    run_cmd("hdfs dfs -rm -r "+ hdfs_base_path)
    run_cmd("hdfs dfs -mkdir "+ hdfs_base_path)

    result = subprocess.call(["sh", os.path.join(ABSPATH, './data_distribute.sh'),
                            '--domain_table', domain_table,
                            '--hdfs_base_path', hdfs_base_path,
                            '--class_name', class_name])
    if result!= 0:
        raise ValueError("数据传到hdfs任务执行失败")
    
    # T2 进行模型分类
    res = run_cmd('hdfs dfs -ls ' + hdfs_base_path + '/model_classified')
    # 如果目录不存在 那么开始跑模型
    if res.returncode != 0:
        # T2.1 进行配置文件的更新
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
        class_name = config_name.replace('_', '-')
        result = subprocess.call(["python3", '-u', run_py_path, class_name + '-templet-classifier',
                                  '1', config_file, class_sh_file_path, 'java-nlp-112701'])
        if result != 0:
            run_cmd('rm -r ' + config_file, raise_err=False)
            raise ValueError("模型分类任务执行失败")
        run_cmd('rm -r ' + config_file, raise_err=False)
    else:
        pass
    # T2.3进行数据上传工作
    result = subprocess.call(["sh", os.path.join(ABSPATH, './data_uploader.sh'),
                              '--classified_table', classified_table,
                              '--hdfs_base_path', hdfs_base_path,
                              '--class_name', class_name])
    if result != 0:
        raise ValueError("分类成果上传执行失败")
    # T3 删除hdfs目录数据
    run_cmd('hdfs dfs -rm -r ' + hdfs_base_path)



