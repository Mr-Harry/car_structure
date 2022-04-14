import traceback
import sys
import re
try:
    import pymysql
except Exception as e:
    print(e)


def import_class(import_str):
    mod_str, _sep, class_str = import_str.rpartition('.')
    __import__(mod_str)
    try:
        return getattr(sys.modules[mod_str], class_str)
    except AttributeError:
        raise ImportError(
            'Class %s cannot be found (%s)' %
            (class_str, traceback.format_exception(*sys.exc_info())))


def import_object(import_str, *args, **kwargs):
    return import_class(import_str)(*args, **kwargs)


def import_fun(import_str):
    return import_class(import_str)


def import_module(import_str):
    __import__(import_str)
    return sys.modules[import_str]



# 规则标注模块
def rule_classifier(msg, classifier_rule_info_dict):
    # 提取信息
    level_1_forward_rule_list = classifier_rule_info_dict.get('level_1_forward_rule_list',{})
    level_2_forward_rule_dict = classifier_rule_info_dict.get('level_2_forward_rule_dict',{})
    level_2_backward_rule_dict = classifier_rule_info_dict.get('level_2_backward_rule_dict',{})
    classifier_rule_label_dict = classifier_rule_info_dict.get('classifier_rule_label_dict',{})
    classifier_rule_ner_dict = classifier_rule_info_dict.get('classifier_rule_ner_dict',{})
    classifier_rule_id_dict = classifier_rule_info_dict.get('classifier_rule_id_dict',{})
    # 进行层级规则匹配
    class_rule_id = '-1'
    class_label = '未识别'
    ner_label = '未识别'
    for level_1_forward_rule in level_1_forward_rule_list:
        if re.search(level_1_forward_rule,msg):
            for level_2_forward_rule in level_2_forward_rule_dict.get(level_1_forward_rule,[]):
                if re.search(level_2_forward_rule,msg):
                    level_2_backward_rule = level_2_backward_rule_dict.get(level_1_forward_rule+'_'+level_2_forward_rule,'.')
                    if level_2_backward_rule == '.' or re.search(level_2_backward_rule,msg) is None:
                        class_rule_id = classifier_rule_id_dict.get(level_1_forward_rule+'_'+level_2_forward_rule+'_'+level_2_backward_rule,'未识别')
                        class_label = classifier_rule_label_dict.get(class_rule_id,'未识别')
                        ner_label = classifier_rule_ner_dict.get(class_rule_id,'未识别')
                        return class_rule_id,class_label,ner_label
    return class_rule_id,class_label,ner_label

# 规则标注模块
def hash_classifier(hashcode, classifier_hash_info_dict):
    # 提取信息
    classifier_hash_label_dict = classifier_hash_info_dict.get('classifier_hash_label_dict',{})
    classifier_hash_ner_dict = classifier_hash_info_dict.get('classifier_hash_ner_dict',{})
    # 进行碰撞匹配
    class_label = classifier_hash_label_dict.get(hashcode,'未识别')
    ner_label = classifier_hash_ner_dict.get(hashcode,'未识别')
    return class_label,ner_label


class DataTool(object):
    def __init__(self,host,port,username,password,database):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        
    def run_sql(self, sql):
        try:
            db = pymysql.connect(host=self.host,
                         port=int(self.port),
                         user=self.username,
                         passwd=self.password,
                         db=self.database)
            cursor = db.cursor()
            if type(sql) == str:
                cursor.execute(sql)
            elif type(sql) == list:
                for _sql in sql:
                    cursor.execute(_sql)
            else:
                raise Exception("Type ERROR")
            data = cursor.fetchall()
            db.commit()
            db.close()
            return data
        except Exception as e:
            print(e)
            return False
    def save_data(self, sql,data):
        try:
            db = pymysql.connect(host=self.host,
                         port=int(self.port),
                         user=self.username,
                         passwd=self.password,
                         db=self.database)
            cursor = db.cursor()
            cursor.executemany(sql, data)
            data = cursor.fetchall()
            db.commit()
            db.close()
            return data
        except Exception as e:
            print(e)
            return False

