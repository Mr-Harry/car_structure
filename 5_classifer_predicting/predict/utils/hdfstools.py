# coding:utf-8
import os
from krbcontext import krbcontext
from hdfs.ext.kerberos import KerberosClient


class HDFSClient(KerberosClient):
    """
    拓展https://hdfscli.readthedocs.io/en/latest/api.html#hdfs.client.Client
    """
    def __init__(self, *args, **kwargs):
        """
        初始化client，使用kerberosclient连接hdfs，本质继承了hdfs中的Client类，拥有该类的所有方法
        """
        url='http://10.10.10.12:50070'
        keytab_file='/home/sf/sf.keytab'
        principal='sf@EXAMPLE.COM'
        hostname='dz-hadoop-nn2'
        with krbcontext(using_keytab=True, keytab_file=keytab_file,principal=principal):
            self.client = KerberosClient(url, hostname_override=hostname)
            return super(HDFSClient, self).__init__(url, hostname_override=hostname, *args, **kwargs)
        
    def walk_file(self, path, *args, **kwargs):
        """
        查找某个文件夹或者文件路径下包含的所有文件，并返回绝对路径
        param：
            path: string, 文件夹或者文件路径
        return: 
            list, 文件夹path下所有的文件列表
        """
        files = []
        for _path, _dir, _file in self.client.walk(path, *args, **kwargs):
            for elem in _file:
                files.append(os.path.join(_path, elem))
        return files
    
    def load_from_hive(self, sql_query, save_path):
        """
        利用hive从表中查找数据并下载到HDFS
        param:
            sql_query: string, 查询语句
            save_path: string, 查询结果在HDFS上的保存路径
        return: 
            None
        """
        exce_query = '''hive -e "insert overwrite  directory '{}' {}" '''.format(save_path, sql_query)
        try:
            os.system(exce_query)
        except Exception as e:
            print(e)
        else:
            print("query is loading done. ")

    def read_generator(self, file_path, chunknum=10240, encoding='utf-8', sep=None):
        """
        利用生成器读取HDFS文件，防止内存过大
        param:
            file_path: string, 读取的文件路径
            chunknum: int, 每次读取的行数
            encoding: string, 读取文件的编码方式
        yeild:
            list, 文件file_path中的chunknum行
        """
        data = []
        with self.client.read(file_path, encoding=encoding) as f:
            for i, line in enumerate(f):
                if sep is not None:
                    data.append(line.rstrip('\n').split(sep))
                    if (i + 1) % chunknum == 0:
                        yield data, len(data)
                        data = []
                else:
                    data.append(line)
                    if (i + 1) % chunknum == 0:
                        yield ''.join(data), len(data)
                        data = []
            if sep is not None:
                yield data, len(data)
            else:
                yield ''.join(data), len(data)


if __name__ == "__main__":
    import time
    import datetime

    start = datetime.date(2019, 11, 1)
    the_dates = [str(start+datetime.timedelta(i)) for i in range(2)]
    for the_date in the_dates:
        print(datetime.datetime.now())
        start_time = time.time()

        client = HDFSClient()
        sql = "SELECT row_key, app_name, phone_id, event_time, msg, main_call_no FROM dps.dps_txt_normal WHERE the_date = '{}'".format(the_date)
        save_path = '/user/sf/lzc/temp/{}'.format(the_date)

        client.load_from_hive(sql, save_path)
        client.download(save_path, '/home/sf/lzc/DataTemp', overwrite=False, n_threads=20)

        print(datetime.datetime.now())
        print("time cost : {}s ".format(time.time() - start_time))
