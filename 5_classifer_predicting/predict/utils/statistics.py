import string
import random

class Task(object):

    '''
    @默认从hive表中，按照数据分区id拉数据到hdfs上，后续支持直接跑本地数据
    '''

    def __init__(self, t_name, t_type, t_data=None):
        self.task_name = t_name
        self.task_type = t_type
        #self.data = t_data
        random.sample(string.digits + string.ascii_letters, 16)
        mykey = "".join(random.sample(
            string.digits + string.ascii_letters, 16))
        self.task_id_prefix = mykey

    def set_id(self, task_date):
        self.task_id = self.task_id_prefix + '-' + task_date

    def set_record_data_id(self, record_data_id):
        self.record_data_id = record_data_id

    def set_cost(self, start_time, finish_time):
        self.start_time = start_time
        self.finish_time = finish_time

    def set_input(self, input_cnt):
        self.input_cnt = input_cnt

    def set_output(self, output_cnt):
        self.output_cnt = output_cnt

class StatisticsBase(object):

    def __init__(self, name):
        self.name = name

    def set_task(self,task_id):
        self.task_id = task_id

    def set_data_id(self,data_id):
        self.data_id = data_id
    
    '''
    time format yyyyMMDD HH:MM:SS
    '''
    def set_start_time(self, start_time_str):
        self.start_time = start_time_str

    '''
    time format yyyyMMDD HH:MM:SS
    '''
    def set_finish_time(self, finish_time_str):
        self.finish_time = finish_time_str


class DataProcessStatistics(StatisticsBase):
    
    def __init__(self, name):
        super(DataProcessStatistics,self).__init__(name)
        self.task_id = None
        self.input_cnt = 0
        self.valid_input_cnt = 0
        self.normal_input_cnt = 0
        self.activated = 1

    def set_input_cnt(self, input_cnt):
        self.input_cnt = input_cnt

    def set_valid_input_cnt(self, valid_input_cnt):
        self.valid_input_cnt = valid_input_cnt
    
    def set_invalid_input_cnt(self, invalid_input_cnt):
        self.invalid_input_cnt = invalid_input_cnt

    def set_normal_input_cnt(self, normal_input_cnt):
        self.normal_input_cnt  = normal_input_cnt

    def set_non_normal_input_cnt(self):
        self.non_normal_input_cnt = self.input_cnt - self.normal_input_cnt

    '''
    @is_activated 0 or 1
    '''
    def set_activated(self,is_activated):
        self.activated = is_activated

'''
for first industry classification
'''
class IndustryNodeStatistics(StatisticsBase):

    def __init__(self, name):
        super(IndustryNodeStatistics,self).__init__(name)
        self.module_name = name
        self.module_id = name
        self.input_cnt = 0
        self.output_cnt = 0
    
    def set_model_info(self, model_name, model_version):
        self.model_name = model_name
        self.model_version = model_version

    def set_input_cnt(self, input_cnt):
        self.input_cnt = input_cnt

    def set_output_cnt(self, output_cnt):
        self.output_cnt = output_cnt
    
'''
for second industry classification 
'''
class BehaviourNodeStatistics(IndustryNodeStatistics):

    def __init__(self, name):
        super(BehaviourNodeStatistics,self).__init__(name)
    
    def set_parent_node(self, parent_node_id):
        self.parent_node_id = parent_node_id


'''
for ner
'''
class  NerNodeStatistics(IndustryNodeStatistics):
    
    def __init__(self, name):
        super(NerNodeStatistics,self).__init__(name)


class DataStatistics(object):
    def __init__(self, name):
        self.name = name
        self.preprocess_stat = None
        self.general_stat = None
        self.middle_stat =None
        self.leaf_stat = None
    
    def set_preprocess_stat(self, preprocess_stat):
        self.preprocess_stat = preprocess_stat
    
    def set_general_stat(self, general_stat):
        self.general_stat = general_stat
    
    def set_middle_stat(self, middle_stat):
        self.middle_stat = middle_stat

    def set_leaf_stat(self, leaf_stat):
        self.middle_stat = leaf_stat


class NodeStatistics(StatisticsBase):
    def __init__(self,name):
        super(NodeStatistics,self).__init__(name)
        #self.total_input = 0
        #self.total_output = 0
        #self.statistics = dict()
        #self.date = 0
        self.init_pararms()

    def set_total_input(self,total_input):
        self.total_input = total_input

    def set_classification_input(self, total_input, valid_input):
        self.classification_total_input = total_input
        self.classification_valid_input = valid_input

    def set_classification_output(self, output):
        self.classification_output = output

    def set_ner_input(self, total_input, valid_input=0):
        self.ner_total_input = total_input
        #self.ner_valid_input = valid_input
    
    def set_ner_output(self, output):
        self.ner_output = output

    def set_statistics(self, output_data_dict):
        self.statistics = output_data_dict

    def set_total_output(self,total_output):
        self.total_output = total_output

    def set_date(self, date):
        self.date = date
    
    def set_classification_cost(self, start,finish):
        self.classification_start = start
        self.classification_finish = finish
    
    def set_ner_cost(self, start, finish):
        self.ner_start = start
        self.ner_finish = finish

    def init_pararms(self):
        self.total_input = 0
        self.total_output = 0
        self.classification_total_input = 0
        self.classification_valid_input = 0
        self.classification_output = 0
        self.ner_total_input = 0
        #self.ner_valid_input = None
        self.date = 0
        self.ner_output = 0
        self.statistics = dict()
        self.classification_start = None
        self.classification_finish = None
        self.ner_start = None
        self.ner_finish = None
        
        

    def to_string(self, field_sep='\t'):
        return field_sep.join([self.name, self.date, str(self.total_input), str(self.total_output),
            str(self.classification_total_input), str(self.classification_valid_input),str(self.classification_output),
            str(self.ner_total_input), str(self.ner_output),str(self.statistics)])





        



    
