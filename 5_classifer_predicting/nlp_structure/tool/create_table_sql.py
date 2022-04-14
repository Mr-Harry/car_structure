import json
from string import Template
import argparse
import os

sql_template_class = Template('''
-- ${config_name} 分类 hive表
drop table if exists ${table_name};
create table if not exists ${table_name}
(
    row_key String COMMENT '唯一编码',
    mobile_id String COMMENT '手机号映射id',
    event_time String COMMENT '发信时间',
    app_name String COMMENT '清洗签名',
    suspected_app_name String COMMENT '原始签名',
    msg String COMMENT '短信内容',
    main_call_no String COMMENT '发信号码',
    abnormal_label String COMMENT '是否为正常文本',
    hashcode String COMMENT 'msg的simhash编码',
    class_rule_id String COMMENT '分类标识来源 -1代表模型 0代表HASH 其它正整数代表相应的规则id',
    ner_label String COMMENT '实体处理方案标识 目前使用normal作为通用',
    class_label_prob Double COMMENT '分类概率值 如果是非模型直接使用1.0'
)COMMENT '${config_name}' partitioned BY(
    the_date string COMMENT '业务日期yyyy-MM-dd格式',
    file_no string COMMENT 'file_no',
    class_label string comment '分类标识')
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
STORED AS orc;
''')


sql_template_domain = Template('''
-- ${config_name} 目标数据获取 hive表
drop table if exists ${table_name};
create table if not exists ${table_name}
(
    row_key String COMMENT '唯一编码',
    mobile_id String COMMENT '手机号映射id',
    event_time String COMMENT '发信时间',
    app_name String COMMENT '清洗签名',
    suspected_app_name String COMMENT '原始签名',
    msg String COMMENT '短信内容',
    main_call_no String COMMENT '发信号码',
    abnormal_label String COMMENT '是否为正常文本',
    hashcode String COMMENT 'msg的simhash编码'
)COMMENT '${config_name}' partitioned BY(
    the_date string COMMENT '业务日期yyyy-MM-dd格式',
    file_no string COMMENT 'file_no')
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
STORED AS orc;
''')


sql_template_nlp = Template('''
-- ${config_name} ${class_name} hive表
drop table if exists ${table_name};
create table if not exists ${table_name}
(
    row_key string comment '唯一编码'
    ,mobile_id string comment '手机号映射id'
    ,event_time string comment '发信时间，yyyy-MM-dd hh24:mi:ss取实际收到时间'
    ,app_name string comment '清洗签名'
    ,suspected_app_name string comment '原始签名'
    ,msg string comment '短文本内容'
    ,main_call_no string comment '发信号码'
    ,abnormal_label string comment '是否为正常文本'
    ,hashcode string comment 'msg的simhash编码'
    ,class_rule_id string comment '分类标识来源，-1代表模型，0代表HASH，其它正整数代表相应的规则id'
    ,class_label string comment '分类标签'
    ,class_label_prob double COMMENT '分类概率值 如果是非模型直接使用1.0'
    ,ner_label string COMMENT '实体处理方案标识'
    ${ner_col}
)COMMENT '${class_name}' partitioned BY(
    the_date string COMMENT '业务日期yyyy-MM-dd格式',
    file_no string COMMENT 'file_no')
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
STORED AS orc;
''')


sql_template_dwb = Template('''
-- ${config_name} ${class_name} hive表
drop table if exists ${table_name};
create table if not exists ${table_name}
(
    ${dwb_columns}
    ${dwb_col}
)COMMENT '${class_name}' partitioned BY(
    the_date string COMMENT '业务日期yyyy-MM-dd格式',
    file_no string COMMENT 'file_no')
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
STORED AS orc;
''')


dwb_columns = {
    'row_key': "row_key string comment '唯一编码'",
    'mobile_id': "mobile_id string comment '手机号映射id'",
    'event_time': "event_time string comment '发信时间，yyyy-MM-dd hh24:mi:ss取实际收到时间'",
    'app_name': "app_name string comment '清洗签名'",
    'suspected_app_name': "suspected_app_name string comment '原始签名'",
    'msg':"msg string comment '短文本内容'",
    'main_call_no': "main_call_no string comment '发信号码'",
    'class_label': "class_label string comment '分类标签'",
    'class_label_prob': "class_label_prob double COMMENT '分类概率值 如果是非模型直接使用1.0'"
}

dwb_columns_sort = ['row_key',
                    'mobile_id',
                    'event_time',
                    'app_name',
                    'suspected_app_name',
                    'msg',
                    'main_call_no',
                    'class_label',
                    'class_label_prob']





def nlp_gen(d, d_l):
    s = ''
    for _l in d_l:
        s += ", {0} string comment '{1}'\n".format(d[_l], _l)
    return s


def dwb_gen(d1, d2):
    def _find(v, d):
        for k in d.keys():
            if d[k] == v:
                return k
        return ''
    s = ''
    for k in d2.keys():
        s += ", {0} string comment '{1}'\n".format(k, _find(k, d1))
    return s


def dwb_columns_gen(drop_column):
    print(drop_column)
    for _column in drop_column:
        print(_column)
        dwb_columns_sort.remove(_column)
    s = ''
    for k in dwb_columns_sort:
        s += ", {0}\n".format(dwb_columns[k])
    s = s[1:]
    return s     

if __name__ == "__main__":
    # 定义参数
    parser = argparse.ArgumentParser(description="建表sql生成")
    parser.add_argument('--config_name', default=None,
                        dest='config_name', type=str, help='配置文件夹名')
    parser.add_argument('--class_name', default=None,
                        dest='class_name', type=str, help='项目名')
    parser.add_argument('--out_file', default=None,
                        dest='out_file', type=str, help='输出文件名')
    parser.add_argument('--config_type', default='master',
                        dest='config_type', type=str, help='配置文件类型')
    args = parser.parse_args()

    config_file = os.path.join(
        args.config_name, 'config_'+args.config_type+'.json')
    col_config_file = os.path.join(args.config_name, 'col_config.json')

    config = json.load(open(config_file, 'r', encoding='utf-8'))
    nlp_dict = config['nlp_table_dict']
    dwb_dict = config['dwb_table_dict']
    drop_column = config.get('drop_column', [])
    col_dict = json.load(open(col_config_file, 'r', encoding='utf-8'))

    sql_list = []
    nlp = []
    dwb = []
    columns_dwb = dwb_columns_gen(drop_column)
    for class_name in col_dict.keys():
        col_ner = nlp_gen(
            col_dict[class_name]['ner2nlp_col'], col_dict[class_name]['ner_list'])
        col_dwb = dwb_gen(
            col_dict[class_name]['ner2nlp_col'], col_dict[class_name]['nlp2dwb'])
        ner_table_name = nlp_dict.get(class_name)
        dwb_table_name = dwb_dict.get(class_name)
        nlp.append(sql_template_nlp.substitute(class_name='nlp '+class_name,
                                               table_name=ner_table_name, ner_col=col_ner, config_name=args.class_name))
        dwb.append(sql_template_dwb.substitute(class_name='dwb '+class_name,
                                                table_name=dwb_table_name,dwb_columns = columns_dwb, dwb_col=col_dwb, config_name=args.class_name))
    domain_table = sql_template_domain.substitute(
        config_name=args.class_name, table_name=config['domain_table'])
    class_table = sql_template_class.substitute(
        config_name=args.class_name, table_name=config['classified_table'])

    sql_list.append(domain_table)
    sql_list.append(class_table)
    sql_list.extend(nlp)
    sql_list.extend(dwb)

    with open(args.out_file, 'w', encoding='utf-8') as f:
        for _sql in sql_list:
            f.write(_sql)
            f.write('\n')
        f.write('\n')
