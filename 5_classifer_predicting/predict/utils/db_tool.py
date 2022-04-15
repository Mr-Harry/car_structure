import pymysql

from utils.environment_config import *

environment = 'online'
statistics_db_conf = statistics_db_config.get(environment, None)
hive_db_name = hive_db_name_config.get(environment, None)


class DBTools(object):
    def __init__(self,log=None):
        # create_mysql_connection()
        self.logger = log
    
    def log(self,msg):
        if self.logger is not None:
            self.logger.info(msg)
        else:
            print(msg)

    def create_mysql_connection(self, host='10.10.10.50', username='ds', password='1qaz!QAZ', db_name='monitor',
                                port=3306):
        return pymysql.connect(host=host, user=username, passwd=password, database=db_name, port=port, charset="utf8")

    def remove_duplicate_data(self, data_id):
        connection = self.create_mysql_connection()
        cur = connection.cursor()
        sql_list = [
            "update {} set activated='0' where data_id = \'{}\';".format(statistics_db_conf.get('preprocess'), data_id),
            "update {} set activated='0' where data_id = \'{}\';".format(statistics_db_conf.get('industry'), data_id),
            "update {} set activated='0' where data_id = \'{}\';".format(statistics_db_conf.get('behaviour'), data_id),
            "update {} set activated='0' where data_id = \'{}\';".format(statistics_db_conf.get('ner'), data_id),
            "update {} set activated='0' where data_id = \'{}\';".format(statistics_db_conf.get('task'), data_id)]
        for sql in sql_list:
            cur.execute(sql)
            msg = "clean statistic db {}".format(sql)
            self.log(msg)

        connection.commit()
        cur.close()
        connection.close()

    def insert_into_preprocess_tb(self, preprocess_stat_list):
        connection = self.create_mysql_connection()
        cur = connection.cursor()
        for preprocess_stat in preprocess_stat_list:
            sql = "INSERT INTO {} \
            (id,task_id,data_id,input_count,valid_input_count,invalid_input_count,normal_input_count,non_normal_input_count,start_time,finish_time\
            ) VALUEs (null,\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\')". \
                format(statistics_db_conf.get('preprocess'), preprocess_stat.task_id, preprocess_stat.data_id,
                       preprocess_stat.input_cnt, preprocess_stat.valid_input_cnt,
                       preprocess_stat.invalid_input_cnt, preprocess_stat.normal_input_cnt,
                       preprocess_stat.non_normal_input_cnt, preprocess_stat.start_time, preprocess_stat.finish_time)
            self.log("load into statistic db {}".format(sql))
            cur.execute(sql)
        connection.commit()
        cur.close()
        connection.close()

    def insert_into_industry_tb(self, industry_stat_list):
        connection = self.create_mysql_connection()
        cur = connection.cursor()
        for industry_stat in industry_stat_list:
            sql = "INSERT INTO {} \
            (id,task_id,data_id,module_name,module_id,model_name,model_version,input_count,output_count,start_time,finish_time) \
            VALUEs (null,\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\')". \
                format(statistics_db_conf.get('industry'), industry_stat.task_id, industry_stat.data_id,
                       industry_stat.module_name, industry_stat.module_id, industry_stat.model_name,
                       industry_stat.model_version, industry_stat.input_cnt, industry_stat.output_cnt,
                       industry_stat.start_time, industry_stat.finish_time)
            self.log("load into statistic db {}".format(sql))
            cur.execute(sql)

        connection.commit()
        cur.close()
        connection.close()

    def insert_into_behaviour_tb(self, behaviour_stat_list):
        connection = self.create_mysql_connection()
        cur = connection.cursor()
        for behaviour_stat in behaviour_stat_list:
            sql = "INSERT INTO {} \
             (id,task_id,data_id,module_name,module_id,p_module_id,model_name,model_version,input_count,output_count,start_time,finish_time) \
            VALUEs (null,\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\')". \
                format(statistics_db_conf.get('behaviour'), behaviour_stat.task_id, behaviour_stat.data_id,
                       behaviour_stat.module_name, behaviour_stat.module_id,
                       behaviour_stat.parent_node_id, behaviour_stat.model_name, behaviour_stat.model_version,
                       behaviour_stat.input_cnt, behaviour_stat.output_cnt,
                       behaviour_stat.start_time, behaviour_stat.finish_time)
            self.log("load into statistic db {}".format(sql))
            cur.execute(sql)

        connection.commit()
        cur.close()
        connection.close()

    def insert_into_ner_tb(self, ner_stat_list):
        connection = self.create_mysql_connection()
        cur = connection.cursor()
        for ner_stat in ner_stat_list:
            sql = "INSERT INTO {} \
            (id,task_id,data_id,module_name,module_id,model_name,model_version,input_count,output_count,start_time,finish_time) \
            VALUEs (null,\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\')". \
                format(statistics_db_conf.get('ner'), ner_stat.task_id, ner_stat.data_id, ner_stat.module_name,
                       ner_stat.module_id, ner_stat.model_name,
                       ner_stat.model_version, ner_stat.input_cnt, ner_stat.output_cnt, ner_stat.start_time,
                       ner_stat.finish_time)
            self.log("load into statistic db {}".format(sql))
            cur.execute(sql)
        connection.commit()
        cur.close()
        connection.close()

    def insert_into_task_tb(self, task):
        connection = self.create_mysql_connection()
        cur = connection.cursor()
        sql = "INSERT INTO {} \
            (id,task_id,data_id,input_count,output_count,start_time,finish_time) \
            VALUEs (null,\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\')". \
            format(statistics_db_conf.get('task'), task.task_id, task.record_data_id, task.input_cnt, task.output_cnt,
                   task.start_time, task.finish_time)
        self.log("load into statistic db {}".format(sql))
        cur.execute(sql)
        connection.commit()
        connection.close()
