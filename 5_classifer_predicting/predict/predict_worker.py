import argparse
import copy
import gc
import logging
import multiprocessing
import os
import pickle
import queue
import time
import traceback
import uuid
import trio
from multiprocessing import Process
import msgpack

import mxnet as mx
import select
# from pynng import Pair1, Respondent0
import zmq
from utils.command import *
from utils.util import pack, unpack


from ner.predict.predict import Predict

parser = argparse.ArgumentParser(description='NER 推理 Worker')

parser.add_argument('--root_path',
                    type=str,
                    default='./data/',
                    help='模型参数存放根目录，默认是 ./data/')


parser.add_argument('--host_ip',
                    type=str,
                    default='tcp://127.0.0.1:4546',
                    help='数据服务器通讯地址，默认是：tcp://127.0.0.1:4546')

parser.add_argument('--dish_ip',
                    type=str,
                    default='tcp://127.0.0.1:7879',
                    help='数据服务器通讯地址，默认是：tcp://127.0.0.1:30000')


parser.add_argument('--batch_size',
                    type=int,
                    default=1024,
                    help='Batch size大小，默认是1024')
parser.add_argument('--checkpoints_num', type=str,
                    default='best', help='加载的模型参数的序号，默认是best')

parser.add_argument('--log_file',
                    type=str,
                    default='./log',
                    help='log文件的存放地址,默认为：./log')

parser.add_argument('--gpu',
                    type=int,
                    default=None,
                    help='设置使用的GPU序号')


parser.add_argument("--is_amp", help="是否启用混合精度",
                    action="store_true")


class ModelRequestCommand(BaseCmd):
    def __init__(self, cmd='send_request_data', log=None):
        super(ModelRequestCommand, self).__init__(cmd=cmd)
        self.log = log

    async def Work(self, **params):
        if params['main'].status['request'].value == 3:
            await params['main'].status['request'].acquire()
            params['main'].status['time'] = time.time()
            send_data = pack('request_data', '', b'')
            params['pipe'].send(send_data)
            # msg = '发送数据请求'
            # if self.log is not None:
            #     self.log.info(msg)
            # else:
            #     print(msg)


class ModelSendPreDataCommand(BaseCmd):
    def __init__(self, cmd='send_pre', log=None):
        super(ModelSendPreDataCommand, self).__init__(cmd=cmd)
        self.log = log

    async def Work(self, **params):
        if not params['main'].pre_data_queu.empty():
            try:
                _data = params['main'].pre_data_queu.get_nowait()
                data_id = _data[0]
                data = _data[1]
            except queue.Empty:
                data = False
            if data:
                send_data = pack('pre_data', data_id, msgpack.dumps(data))
                params['pipe'].send(send_data)


class ModelRecvDataCommand(BaseCmd):
    def __init__(self, cmd='data', log=None):
        super(ModelRecvDataCommand, self).__init__(cmd=cmd)
        self.log = log

    async def Work(self, **params):
        if params['main'].status['request'].value == 2:
            await params['main'].status['request'].acquire()
            if params['data'] != b'error' or params['data_id'] != 'error':
                data_id = params['data_id']
                data, file_name, out_path, model_name, msg_num = msgpack.loads(
                    params['data'])
                self.log.info('接到数据: ' + data_id)
                try:
                    if model_name != params['main'].Prd_name:
                        self.log.info("加载 " + model_name + ' 模型')
                        params['main'].Prd_name = model_name
                        if params['main'].Prd is not None:
                            del params['main'].Prd
                            gc.collect()
                            params['main'].ctx.empty_cache()
                        params['main'].Prd = Predict(
                            root_path=os.path.join(
                                params['main'].model_path, model_name),
                            batch_size=params['main'].batch_size,
                            checkpoints_num=params['main'].checkpoints_num,
                            CTX=params['main'].ctx,
                            is_amp=params['main'].is_amp,
                            log=self.log)
                        params['main'].tag = copy.deepcopy(
                            params['main'].Prd.tag)
                        params['main'].i2t = copy.deepcopy(
                            params['main'].Prd.tagvocab.idx_to_token)
                        params['main'].mode = params['main'].Prd.mode
                        # _f1_path = os.path.join(params['main'].model_path, model_name,
                        #                         'best_F1.pkl')
                        # if os.path.isfile(_f1_path):
                        #     params['main'].F1 = pickle.load(open(_f1_path, 'rb'))
                        # else:
                        #     params['main'].F1 = None

                    _pre_data, valid_lengths, datas = params['main'].Prd.new_predict(
                        data, msg_num)
                    pre_data = [file_name, params['main'].i2t, params['main'].tag,
                                params['main'].mode, data, _pre_data, out_path, valid_lengths,
                                datas]
                    params['main'].pre_data_queu.put(
                        [data_id, pre_data])
                except FileNotFoundError:
                    send_data = pack('exit', '',b'')
                    params['pipe'].send(send_data)
                except:
                    try:
                        del params['main'].Prd
                    except AttributeError:
                        pass
                    params['main'].Prd_name = None
                    params['main'].Prd = None
                    self.log.error('模型错误退出！\n' + traceback.format_exc())
                    send_data = pack('retry_data', params['data_id'],params['data'])
                    params['pipe'].send(send_data)
            else:
                time.sleep(2)
            params['main'].status['request'].release()
            params['main'].status['request'].release()


class CloseCommand(BaseCmd):
    def __init__(self, cmd='done', log=None):
        super(CloseCommand, self).__init__(cmd=cmd)
        self.log = log

    async def Work(self, **kwargs):
        # kwargs['pipe'].send(b'work closeing!')
        self.log.info('work closeing')
        kwargs['cancel_scope'].cancel()
        kwargs['main'].exit_flag.set()
        kwargs['main'].close()


class Predict_Worker(Process):
    def __init__(self, model_path,
                 batch_size=1024,
                 ctx=mx.gpu(0),
                 server_ip='tcp://127.0.0.1:6666',
                 dish_ip='tcp://127.0.0.1:30000',
                 checkpoints_num='best',
                 _id='',
                 is_amp=False,
                 log=None):
        super().__init__()
        self.exit_flag = multiprocessing.Event()
        self.id = _id
        self.batch_size = batch_size
        self.model_path = model_path
        self.server_ip = server_ip
        self.dish_ip = dish_ip
        self.log = log
        self.ctx = ctx
        self.checkpoints_num = checkpoints_num
        self.is_amp = is_amp

        self.cmd_analy = CmdAnaly()
        self.cmd_analy.add(ModelRecvDataCommand(log=self.log))
        self.cmd_analy.add(ModelRequestCommand(log=self.log))
        self.cmd_analy.add(ModelSendPreDataCommand(log=self.log))
        self.cmd_analy.add(CloseCommand(log=self.log))

    def run(self):
        self.log.info(str(self.id) + ' 号模型启动!')
        self.Prd_name = None
        self.Prd = None
        self.tag = None
        self.i2t = None
        # self.F1 = None
        self.mode = None
        self.zctx = zmq.Context()
        semaphore = trio.Semaphore(initial_value=3)
        self.status = {'request': semaphore, 'time': time.time()}
        self.pre_data_queu = queue.Queue()
        self.server_client = self.zctx.socket(zmq.CLIENT)
        self.server_client.connect(self.server_ip)

        self.dish = self.zctx.socket(zmq.DISH)
        self.dish.connect(self.dish_ip)
        self.dish.join("done")

        self.poller = zmq.Poller()

        self.poller.register(self.dish, zmq.POLLIN)
        self.poller.register(self.server_client, zmq.POLLIN | zmq.POLLOUT)

        self.socket_list_send = {
            self.server_client: ['send_request_data', 'send_pre']
        }
        try:

            async def run():
                with trio.CancelScope() as cancel_scope:
                    async with trio.open_nursery() as nursery:
                        while not self.exit_flag.is_set():
                            await trio.sleep(.1)
                            events = self.poller.poll(2)
                            if events:
                                for socket, fd in events:
                                    if fd == zmq.POLLOUT:
                                        msgs = self.socket_list_send[socket]
                                        params = {'main': self}
                                        params['pipe'] = socket
                                        for msg in msgs:
                                            nursery.start_soon(
                                                self.cmd_analy.Analy, msg, params)
                                    else:
                                        rece_time = time.time()
                                        recv_data = socket.recv()
                                        msg, data_id, data = unpack(recv_data)
                                        params = {'main': self}
                                        params['data'] = data
                                        params['pipe'] = socket
                                        params['cancel_scope'] = cancel_scope
                                        params['data_id'] = data_id
                                        nursery.start_soon(
                                            self.cmd_analy.Analy, msg, params)
            trio.run(run)
        except:
            self.log.error('xxxx\n' + traceback.format_exc())
            send_data = pack('exit', '',b'')
            server_client.send(send_data)

    def close(self):
        self.exit_flag.set()
        self.log.info('work closed!')


if __name__ == "__main__":
    args = parser.parse_args()
    _id = str(uuid.uuid4())
    log = logging.getLogger('NER_Predict_Worker: '+_id[:5])
    log.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        fmt='%(levelname)s:%(name)s:%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    fh = logging.FileHandler(args.log_file, mode='w')
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)
    log.addHandler(console)
    log.addHandler(fh)

    log.info(args)

    root_path = args.root_path
    batch_size = args.batch_size
    host_ip = args.host_ip
    dish_ip = args.dish_ip

    checkpoints_num = args.checkpoints_num
    CTX = mx.cpu() if args.gpu is None else mx.gpu(args.gpu)

    is_amp = args.is_amp
    try:

        PW = Predict_Worker(model_path=root_path, batch_size=batch_size, ctx=CTX,
                            server_ip=host_ip, dish_ip=dish_ip, checkpoints_num=checkpoints_num, _id=_id, is_amp=is_amp, log=log)

        PW.start()
        PW.join()
    except:
        log.error('错误退出！\n' + traceback.format_exc())
