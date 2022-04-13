import sys
import os
import json
from utils import import_fun
from functools import partial

# class_dict={
#     "bank_v2":"银行",
#     "financial_internet_v2":"互联网金融",
#     "investment_v2":"理财",
#     "post_v2":"快递"
# }


def target_data_extractor(msg, app_name, suspected_app_name, hashcode, abnormal_label, dict_list, domain_extractor):
    return domain_extractor(msg, app_name, suspected_app_name, hashcode, abnormal_label, dict_list)

# 目标数据抽取函数封装
def target_data_extractor_generator(dict_list, domain_extractor):
    return partial(target_data_extractor, dict_list=dict_list, domain_extractor=domain_extractor)
# def multi_classifer(class_dict,msg, app_name, suspected_app_name, hashcode, abnormal_label, dict_list={}):

class Classifer(object):
    def __init__(self,  class_dict):
        self.funs={}
        for configname,v in class_dict.items():
            dict_list_file = os.path.join('config', configname, 'dict_list_file.json')
            dict_list = json.load(open(dict_list_file, 'r', encoding='utf-8')
                          ) if dict_list_file is not None else []
            domain_extractor = import_fun(
                'config.' + configname +'.domain_extractor.domain_extractor')
            self.funs[configname] = target_data_extractor_generator(dict_list, domain_extractor)
        self.class_dict=class_dict
    
    def multi_class(self, *args):
        return ",".join([v for k, v in self.class_dict.items() if self.funs[k](*args)]) 



