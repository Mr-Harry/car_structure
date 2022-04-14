import os
import json
import subprocess
import argparse
import time
import sys
ABSPATH = os.path.dirname(os.path.abspath(__file__))


parser = argparse.ArgumentParser(description="正式任务")

parser.add_argument('--the_date', default=None,
                    dest='the_date', type=str, help='the_date')
parser.add_argument('--file_no', default=None,
                    dest='file_no', type=str, help='file_no')
parser.add_argument('--task_type', default='master',
                    dest='task_type', type=str, help='task_type')
parser.add_argument('--class_name', type=str, default=None, help='class_name')

parser.add_argument('--start_num', type=int, default=0, help='start_num')

args = parser.parse_args()
the_date = args.the_date
file_no = args.file_no
task_type = args.task_type
start_num = args.start_num
class_name = args.class_name
if start_num not in [0, 1, 2, 3]:
    raise ValueError("start_num 错误")
flags = 0

flags_result = 1
for task_id in range(start_num, 4):
    flags_result = 1
    while flags < 2:
        print('开始 {0} 类目 the_date={1}, file_no={2}, task_type={3} 的 结构化任务 {4}'.format(
            class_name, the_date, file_no, task_type, task_id))
        # 给result一个默认值
        result = 1
        if task_id == 0:
            result = subprocess.call(["python3", "-u", os.path.join(ABSPATH, 'domain/domain.py'),
                                      '--config_name', class_name, '--the_date', the_date, '--file_no', file_no, '--config_type', task_type])
        # 如果是数据分类任务
        elif task_id == 1:
            result = subprocess.call(["python3", "-u", os.path.join(ABSPATH, 'classifier/classifier.py'),
                                      '--config_name', class_name, '--the_date', the_date, '--file_no', file_no, '--config_type', task_type])
        # 如果是实体抽取任务
        elif task_id == 2:
            result = subprocess.call(["python3", "-u", os.path.join(ABSPATH, 'ner/ner.py'),
                                      '--config_name', class_name, '--the_date', the_date, '--file_no', file_no, '--config_type', task_type])
        # 如果是数据清洗任务
        elif task_id == 3:
            result = subprocess.call(["python3", "-u", os.path.join(ABSPATH, 'cleaner/cleaner.py'),
                                      '--config_name', class_name, '--the_date', the_date, '--file_no', file_no, '--config_type', task_type])
        if result != 0:
            print('{0} 类目 the_date={1}, file_no={2},task_type={3} 的 结构化任务 {4} 步骤失败，尝试重试！'.format(
                class_name, the_date, file_no, task_type, task_id))
            # 如果任务失败 休息10分钟
            time.sleep(60)
            flags += 1
            continue
        else:
            print('{0} 类目 the_date={1}, file_no={2}, task_type={3} 的 结构化任务 {4} 步骤执行成功'.format(
            class_name, the_date, file_no, task_type, task_id))
            time.sleep(5)
            flags_result = 0
            break
    if result != 0:
        print('{0} 类目 the_date={1}, file_no={2}, task_type={3} 的 结构化任务 {4} 步骤失败，任务推出！'.format(
            class_name, the_date, file_no, task_type, task_id))
        sys.exit(-1)

sys.exit(flags_result)
