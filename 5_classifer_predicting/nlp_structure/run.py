import sys
import time
import requests
from minio import Minio, ResponseError
import signal

minio_host = '10.30.103.10:9000'
server_host = 'http://10.30.103.11:9080'

pod_name = ''
zmq_ip = ''


class Client:
    bucket = 'kele-job-submit'
    count = 0
    retry = 50

    def __init__(self):
        self.minio_client = Minio(minio_host,
                                  access_key="admin",
                                  secret_key="123123123",
                                  secure=False)

    def upload(self, minio_path, local_absolute_path):
        try:
            self.minio_client.fput_object(self.bucket, minio_path, local_absolute_path)
            print('config upload finish')
        except ResponseError as err:
            print(err)

    @staticmethod
    def process_list(lines):
        return [line for line in lines if len(line) > 1]

    def printLog(self):
        try:
            log = requests.get(server_host + '/k8s/logs?podName=' + pod_name).json()['data']
            lines = log.split('\n')
            lines = lines[self.count:]
            self.count += len(lines)
            if len(lines) > 0:
                result = '\n'.join(self.process_list(lines))
                print(result)
        except Exception:
            self.job_retry()

    def get_pod_status(self):
        time.sleep(3)
        try:
            data = requests.get(server_host + '/k8s/pod/status?podName=' + pod_name).json()['data']
            return data
        except Exception:
            self.job_retry()
            return None

    def job_retry(self):
        if self.retry == 0:
            kill_job()
            raise JobException('任务异常 重试次数用尽')

        self.retry = self.retry - 1
        print('任务异常 正在重试，重试剩余次数 = ' + str(self.retry))

    def start_job(self, gpu_limit, config_path, shell_path, i_type):
        get_zmq_ip()
        self.upload(pod_name + '.json', config_path)
        self.upload(pod_name + '.sh', shell_path)
        j = {
            "command": ["/bin/sh"],
            "args": ["-c", "export PATH=.:$PATH;mc cat minio/{} >> config.json;mc cat minio/{} >> run2.sh;bash run2.sh"
                .format(self.bucket + '/' + pod_name + '.json', self.bucket + '/' + pod_name + '.sh')],
            "gpuLimit": gpu_limit,
            "podName": pod_name,
            "zmqAddress": zmq_ip,
            "imageType": i_type
        }

        print('request param !!!!!!!!!!!!!!!!!!!!!!!!!', j)
        while True:
            try:
                rep = requests.post(server_host + "/k8s/createJob", json=j).json()
                pod_create_status = rep['data']['status']
                print('任务创建成功')
                break
            except:
                print('任务创建失败！！！')
                self.job_retry()
                time.sleep(60)

        if isinstance(pod_create_status, str) and pod_create_status == 'Failure':
            print('job start fail')
            print('fail message: {}'.format(str(rep['data']['message'])))
            raise JobException('任务启动失败')
        else:
            print('job start success')
        time.sleep(3)
        print('pod name = {}'.format(pod_name))
        pod_status = self.get_pod_status()
        print('pod_status = {}'.format(pod_status))
        if pod_status.lower() == 'pending' or pod_status.lower() == "scheduling":
            print(pod_status)
            return True
        while True:
            time.sleep(3)
            status = self.get_pod_status()

            if status == 'Failed':
                print('程序运行失败')
                sys.exit(1)

            if status != 'Running' and status is not None:
                self.printLog()
                print("----------")
                print('status = {}'.format(status))
                print('run.py 日志接收完成，运行结束')
                sys.exit(0)
                # echo $?
            else:
                self.printLog()


def kill_job():
    requests.get(server_host + '/k8s/pod/delete?podName={}'.format(pod_name))
    print('kill pod {}'.format(pod_name))


class JobException(Exception):
    def __init__(self, context):
        self.context = context

    def __str__(self):
        return self.context


def generate_name(pn):
    global pod_name
    pod_name = pn + '-' + str(int(time.time() * 1000))


def get_zmq_ip():
    global zmq_ip
    global pod_name
    zmq_ip = requests.get(server_host + "/zmq/start?jobName=" + pod_name).json()['data']


def get_client(name):
    generate_name(name)
    print('pod name = {}'.format(pod_name))
    return Client()


def handler(signalnum, frame):
    kill_job()
    print("收到信号", signalnum, frame)
    exit(1)


if __name__ == '__main__':

    gpu_num = sys.argv[2]
    client = get_client(sys.argv[1])
    signal.signal(signal.SIGTERM, handler)

    try:
        image_type = sys.argv[5]
    except IndexError:
        image_type = 'v1'

    try:
        while True:
            retry_flag = client.start_job(gpu_num, sys.argv[3], sys.argv[4], image_type)
            if retry_flag:
                kill_job()
                time.sleep(300)
            else:
                break
    except KeyboardInterrupt:
        kill_job()
        print('检测到KeyboardInterrupt，删除任务{}'.format(pod_name))
    except JobException as e:
        kill_job()
        print(e)
        sys.exit(1)
