#%%
from ast import arg
import json
import os
from string import Template
import argparse
from dateutil.relativedelta import relativedelta
import datetime
from libs.spark_base_connector import BaseSparkConnector

ABSPATH = os.path.dirname(os.path.abspath(__file__))
ABSPATH_F = os.path.dirname(ABSPATH)

sql_tmp = Template('''
insert into table ${write_table} partition(file_no='${file_no}')
select /*+ BROADCAST (b) */ ${col} from ${read_table} a
left semi join (select mobile_id from ${id_back_date_table}  
where back_date > '${month}' and date_sub(back_date,220) < '${month}' and nvl(mobile_id, '') != ''
    and file_no regexp '${file_no}') b
on a.mobile_id = b.mobile_id
distribute by pmod(hash(1000*rand(1)), 40) 
''')

start_end_sql = Template('''
select min(date_sub(back_date, 220)) as start_date,
    max(back_date) as end_date
from ${id_back_date_table}
where nvl(mobile_id, '') != ''
    and file_no regexp '${pt}'
''')

dwb_columns_sort = ['row_key',
                    'mobile_id',
                    'event_time',
                    'app_name',
                    'suspected_app_name',
                    'msg',
                    'main_call_no',
                    'class_label',
                    'class_label_prob']

class DataCleaner(BaseSparkConnector):
    def __init__(self, app_name, log_level=None):
        """
        数据清洗模块
        Args:
            app_name: 必填参数，用于标记Spark任务名称;  str
            log_level: 选填参数，用于标记Spark任务的日志等级，只可以为WARN、INFO、DEBUG、ERROR其中一种;  str
        """
        # 初始化spark
        super().__init__(app_name=app_name, log_level=log_level)
        # 加载完毕
        self.logger.info('数据清洗模块初始化完成')
    def run(self,c_m,read_col,file_no,index_list,index,id_back_date_table):
        # mobile_df = self.spark.sql(mobile_id_sql.substitute(pt=file_no)).cache()
        # mobile_df.createOrReplaceTempView('tmp_mobile_id_list_')
        start_end_df = self.spark.sql(start_end_sql.substitute(pt=file_no,id_back_date_table=id_back_date_table)).toJSON().collect()
        _start_end = json.loads(start_end_df[0])
        _start = _start_end['start_date']
        _end = _start_end['end_date']
        start = datetime.datetime.strptime(_start,'%Y-%m-%d')
        end = datetime.datetime.strptime(_end,'%Y-%m-%d')
        _the_date = start
        the_dates = []
        while _the_date <= end:
            month = _the_date.strftime("%Y-%m")
            the_dates.append(month)
            _the_date = _the_date + relativedelta(months=+1)
        print(the_dates)
        # self.spark.sql('CACHE TABLE tmp_mobile_id_list AS SELECT mobile_id FROM tmp_mobile_id_list_')
        if index == -1:
            for k in c_m.keys():
                _col = ', '.join([*read_col,*c_m[k][2],'the_date'])
                # partitons = self.spark.sql('show partitions {0}'.format(c_m_dwb[k][1])).collect()
                # partitons_ = []
                # for i in partitons:
                #     d = i[0].split('/')[0].split('=')[1][0:7]
                #     partitons_.append(d)
                self.spark.sql("ALTER TABLE {0} DROP IF EXISTS PARTITION (file_no='{1}')".format(c_m[k][0],file_no))
                for m in the_dates:
                    read_table = "(select * from {0} where the_date regexp '{1}')".format(c_m_dwb[k][1],m)
                    _sql = sql_tmp.substitute(write_table = c_m[k][0],file_no = file_no,col = _col,read_table = read_table,month=m,id_back_date_table=id_back_date_table)
                    print(_sql)
                    self.spark.sql(_sql)
        elif index < len(index_list):
            k = index_list[index]
            print('指定第 {0} 张表 {1} 进行处理。完整排序为：{2}'.format(index,k,str(index_list)))
            _col = ', '.join([*read_col,*c_m[k][2],'the_date'])
            # partitons = self.spark.sql('show partitions {0}'.format(c_m_dwb[k][1])).collect()
            # partitons_ = []
            # for i in partitons:
            #     d = i[0].split('/')[0].split('=')[1][0:7]
            #     partitons_.append(d)
            self.spark.sql("ALTER TABLE {0} DROP IF EXISTS PARTITION (file_no='{1}')".format(c_m[k][0],file_no))
            for m in the_dates:
                read_table = "(select * from {0} where the_date regexp '{1}')".format(c_m_dwb[k][1],m)
                _sql = sql_tmp.substitute(write_table = c_m[k][0],file_no = file_no,col = _col,read_table = read_table,month=m,id_back_date_table=id_back_date_table)
                print(_sql)
                self.spark.sql(_sql)
        else:
            print('index {0} is null '.format(index))


if __name__ == "__main__":
    # 定义参数
    parser = argparse.ArgumentParser(description="建表sql生成")
    parser.add_argument('--config_name', default=None,
                        dest='config_name', type=str, help='配置文件夹名')
    parser.add_argument('--file_no', default='master',
                    dest='file_no', type=str, help='file_no')
    parser.add_argument('--index', default=-1,
                    dest='index', type=int, help='第几张表')
    parser.add_argument('--id_table',default='customer_test.customer_sample_id',type=str,help='id 回溯表',dest='id_table')
    args = parser.parse_args()
    config_name = args.config_name
    file_no = args.file_no
    index = args.index

    customer_json = json.load(open(os.path.join(ABSPATH_F, 'config', config_name,'config_customer.json'),'r'))
    col_json = json.load(open(os.path.join(ABSPATH_F, 'config',config_name,'col_config.json'),'r'))
    master_json = json.load(open(os.path.join(ABSPATH_F, 'config',config_name,'config_master.json'),'r'))
    customer_dwb = customer_json.get('dwb_table_dict')
    master_dwb = master_json.get('dwb_table_dict')
    cu_drop = customer_json.get('drop_column', [])
    ma_drop = master_json.get('drop_column', [])

    cu_col = []
    ma_col = []
    for _column in dwb_columns_sort:
        if _column not in cu_drop:
            cu_col.append(_column)
        if _column not in ma_drop:
            ma_col.append(_column)

    read_col = []
    for i in cu_col:
        if i not in ma_col:
            read_col.append("'' as {0}".format(i))
        else:
            read_col.append(i)
    c_m_dwb = {}
    index_list = []
    for k in customer_dwb.keys():
        _col = col_json.get(k,None)
        dwb_col = list(_col['nlp2dwb'].keys()) if _col is not None else []
        c_m_dwb[k] = [customer_dwb[k],master_dwb[k],dwb_col]
        index_list.append(k)
    index_list.sort()
    
    dr = DataCleaner(app_name = config_name + '_MasterToCustomer')
    dr.run(c_m_dwb,read_col,file_no,index_list,index,args.id_table)

    