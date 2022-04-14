from libs.utils import DataTool
import argparse
import json
import sys
from datetime import timedelta
from datetime import datetime

host="10.10.10.50"
host="10.1.21.81"
port="3306"
port="21003"
user="sf"
passwd="KiYQ0g&CJgo!cgWR"
database="nlp_structure_old"
log_table = "nlp_count_test" # 任务执行成功记录表



if __name__ == "__main__":
    # 定义参数
    parser = argparse.ArgumentParser(description="数据清洗模块")
    parser.add_argument('--read_table', default='nlp_count_test',
                        dest='read_table', type=str, help='源数据表名')
    parser.add_argument('--save_table', default='nlp_data_count_test',
                        dest='save_table', type=str, help='目标数据表名')
    args = parser.parse_args()
    read_table = args.read_table
    save_table = args.save_table

    dt = DataTool(host=host,port=port,username=user,password=passwd,database=database)
    up_time = dt.run_sql("select max(update_time) as up from {0}".format(save_table))
    up  = (up_time[0][0] - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
    the_date = dt.run_sql("select the_date from {0} where update_time > '{1}' group by the_date".format(read_table,up))
    print(the_date)
    the_dates = []
    if len(the_date) < 0:
        sys.exit(0)
    for _t in the_date:
        the_dates.append(_t[0])

    data = dt.run_sql("select count,class_label_count ,class_label_ner_count,the_date,file_no,class_name from {0} where the_date regexp '{1}'".format(read_table,'|'.join(the_dates)))
    count_dict = {}
    cout_label_dict = {}
    cout_label_ner_dict = {}
    for item in data:
        if item[-3] not in count_dict:
            count_dict[item[-3]] = {}
        if item[-1] not in count_dict[item[-3]]:
            count_dict[item[-3]][item[-1]] = 0

        if item[-3] not in cout_label_dict:
            cout_label_dict[item[-3]] = {}
        if item[-1] not in cout_label_dict[item[-3]]:
            cout_label_dict[item[-3]][item[-1]] = {}

        if item[-3] not in cout_label_ner_dict:
            cout_label_ner_dict[item[-3]] = {}
        if item[-1] not in cout_label_ner_dict[item[-3]]:
            cout_label_ner_dict[item[-3]][item[-1]] = {}
        label_data = json.loads(item[1])
        for k in label_data:
            if k not in cout_label_dict[item[-3]][item[-1]]:
                cout_label_dict[item[-3]][item[-1]][k] = 0
            cout_label_dict[item[-3]][item[-1]][k] += label_data[k]
        ner_data = json.loads(item[2])
        for k in ner_data:
            if k not in cout_label_ner_dict[item[-3]][item[-1]]:
                cout_label_ner_dict[item[-3]][item[-1]][k] = {}
            for kk in ner_data[k]:
                if kk not in cout_label_ner_dict[item[-3]][item[-1]][k]:
                    cout_label_ner_dict[item[-3]][item[-1]][k][kk] = 0
                cout_label_ner_dict[item[-3]][item[-1]][k][kk] += ner_data[k][kk]
        count_dict[item[-3]][item[-1]] += item[0]
    cout_label = []
    for k in cout_label_dict:
        for kk in cout_label_dict[k]:
            for kkk in cout_label_dict[k][kk]:
                cout_label.append((kkk,kk,'count',cout_label_dict[k][kk][kkk],k))
                

    count_label_ner = []
    for k in cout_label_ner_dict:
        _d = cout_label_ner_dict[k]
        for kk in _d:
            __d = _d[kk]
            for kkk in __d:
                for kkkk in __d[kkk]:
                    p = 1 - float(__d[kkk][kkkk])/float(cout_label_dict[k][kk][kkk]) if cout_label_dict[k][kk][kkk] > 0 else 0
                    count_label_ner.append((kkk,kk,kkkk,p,k))
    
    all_data = []
    all_data.extend(count_label_ner)
    all_data.extend(cout_label)
    # sqls = []
    # for i in all_data:
    #     _sql = "INSERT INTO nlp_structure_old.{0} (细类目,大类目,字段,数据量,日期) VALUES ('{1}', '{2}', '{3}', {4}, '{5}') ON DUPLICATE KEY UPDATE 细类目 = '{1}',大类目 = '{2}',字段='{3}',日期='{5}';".format(save_table,i[0],i[1],i[2],i[3],i[4])
    #     sqls.append(_sql)
    sql = "INSERT INTO nlp_structure_old.{0} (细类目,大类目,字段,数据量,日期) VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE 细类目 = values(细类目),大类目 = values(大类目),字段=values(字段),数据量=values(数据量),日期=values(日期);".format(save_table)
    print(len(all_data))
    res = dt.save_data(sql,all_data)
    print(res)

