# coding:utf-8
import os
import json
import torch
import numpy as np


class BasicConfig(object):
    def __init__(self):
        # 公共参数配置
        self.ABSOLUTE_PATH = os.path.dirname(os.path.dirname(__file__))
        self.CHECKPOINT_FOLD = os.path.join(self.ABSOLUTE_PATH, 'checkpoint')

    @classmethod
    def model_config(cls, model_name):
        # TEXTCNN参数
        if model_name == 'TEXTCNN':
            cls.KERNEL_SIZE = [1, 2, 3, 4]
            cls.CHANNEL_SIZE = 128

        # DPCNN参数
        if model_name == 'DPCNN':
            cls.CHANNEL_SIZE = 128  # paper默认250
            cls.MIN_BLOCK_SIZE = 2  # 最小尺寸

        # BIRNN参数
        if model_name == 'BIRNN':
            # RNN单元基础参数
            cls.RNN_TYPE = "GRU"  # RNN单元类型('RNN', 'LSTM', 'GRU')
            cls.HIDDEN_DIM = 100
            cls.NUM_LAYER = 2
            cls.NONLINEARITY = "tanh"  # 内部激活函数
            cls.BIAS = True
            cls.RNN_DROPOUT = 0.5  # 每个元素置为0的概率（即drop值）
            cls.BATCH_FITST = True  # 数据的第一维是否为Batch
            cls.BIDIRECTIONAL = True  # 是否双向
            # BIRNN参数
            cls.LINEAR_CHANNEL = None
            # attention维度
            cls.IS_ATTENTION = True  # 是否添加attention层
            cls.ATTENTION_SIZE = 64  # attention节点数
        
        if model_name == 'TransformerLC':
            cls.NHEAD = 8  # 必须整除与self.EMBEDDING_DIM
            cls.NUM_LAYER = 1
        
        if model_name == 'fasttext':
            cls.HIDDEN_DIM = 32
        
        if model_name == 'VDCNN':
            cls.DEPTH = 9

        if model_name == 'SVDCNN':
            cls.DEPTH = 9

    @classmethod
    def optimizer_config(cls, optimizer_name):
        # Adam参数
        if optimizer_name == "Adam":
            cls.LEARNING_RATE = 0.001

        # RMSprop参数
        if optimizer_name == "RMSprop":
            cls.LEARNING_RATE = 0.01

        # SGD"参数
        if optimizer_name == "SGD":
            cls.LEARNING_RATE = 0.01
            cls.MOMENTUM = 0.9

    def pretrained_embedding(self, pretrained):
        embeddings = np.random.rand(self.VOCAB_NUM, self.EMBEDDING_DIM)
        with open(pretrained) as f:
            for line in f.readlines()[1:]:  # 跳过header
                line = line.rstrip().split(' ')
                try:
                    index = self.VOCABS[line[0]]
                    embedding = [float(i) for i in line[1:]]
                    embeddings[index] = np.asarray(embedding, dtype='float32')
                except KeyError:  # 字典中未出现的词直接忽略，用随机的向量代替
                    continue
        self.PRETRAINED = torch.tensor(embeddings, dtype=torch.float32)


class TrainConfig(BasicConfig):
    def __init__(self, args=None):
        super(TrainConfig, self).__init__()
        # 公共参数配置
        self.args = args
        self.LOG_FILE = args.log_file
        self.MODEL_METHOD = args.model_method  # 选用模型方法（TEXTCNN,DPCNN,BIRNN）
        self.MODEL_NAME = args.model_name  # 模型保存名称
        self.TRAIN_DATASET_PATH = args.train_dataset_path  # 训练集路径
        self.SEP = args.sep  # 训练数据中文本和标签之间的分隔符
        self.LABEL_PATH = os.path.join(self.ABSOLUTE_PATH, args.label_path)
        with open(self.LABEL_PATH, 'r', encoding='utf-8') as f:
            self.LABELS = json.load(f)
        self.CLASS_NUM = len(self.LABELS)
        self.N_GRAM = args.n_gram
        self.VOCAB_MAX_NUM = args.vocab_max_num
        self.PAD_DIRECTION = args.pad_direction

        # 训练参数
        self.TRAIN_RATE = args.train_rate  # 训练比例
        self.VALIDATE_RATE = args.validate_rate  # 验证比例
        self.TEST_RATE = args.test_rate  # 测试比例
        self.SEED = args.seed  # 随机种子
        self.UNSAMPLE_NUM = args.unsample_num  # 上采样最小数据，默认0
        self.OPTIMIZER = args.optimizer  # Adam,SGD,RMSprop
        self.NUM_EPOCHS = args.epoch
        self.BATCH_SIZE = args.batch_size
        self.IS_EMBEDDING = args.is_embedding  # 是否加入EMBEDDING层
        self.MAX_LEN = args.max_len  # 每条文本选取的最大长度
        self.EMBEDDING_DIM = args.embedding_dim  # Embedding层的维度
        self.PRETRAINED = None
        self.VOCABS = {'<PAD>': 0, '<UNK>': 1}
        self.VOCAB_NUM = None
        self.NUM_WORKERS = args.num_workers
        self.THRESHOLD_PATH = None
        self.HALF = args.half
        self.WEIGHTED_CLASS = args.weighted_class
        self.LOG_LEVEL = args.log_level  # "debug", "info", "warning", "error", "critical"
        self.SAVE_FOLD = None
        self.K_FOLD = None

        TrainConfig.optimizer_config(self.OPTIMIZER)
        TrainConfig.model_config(self.MODEL_METHOD)


class CrossValTrainConfig(TrainConfig):
    def __init__(self, args=None):
        super(CrossValTrainConfig, self).__init__(args=args)
        # 公共参数配置
        self.K_FOLD = args.k_fold
        self.TRAIN_RATE = 1.0
        self.VALIDATE_RATE = 0.0
        self.TEST_RATE = 0.0


class PredictConfig(BasicConfig):
    """
    预测不设置使用哪个GPU，默认运行在只在1个默认的GPU上
    """
    def __init__(self):
        super(PredictConfig, self).__init__()
        with open(os.path.join(self.ABSOLUTE_PATH, 'config/config.json'), 'r', encoding='utf-8') as f:
            args = json.load(f)

        # 公共参数配置
        self.MODEL_METHOD = args.get('model_method')
        self.BATCH_SIZE = 2048
        self.HALF = True
        self.PRETRAINED = None

        with open(os.path.join(self.ABSOLUTE_PATH, args.get('label_path')), 'r', encoding='utf-8') as f:
            self.LABELS = json.load(f)
        
        self.CLASS_NUM = len(self.LABELS)
        self.N_GRAM = args.get('n_gram')
        self.VOCAB_MAX_NUM = args.get('vocab_max_num')
        self.PAD_DIRECTION = args.get('pad_direction')

        self.LOG_LEVEL = args.get('log_level')
        self.LOG_FILE = None
        self.IS_EMBEDDING = args.get('is_embedding')
        self.EMBEDDING_DIM = args.get('embedding_dim')
        self.MAX_LEN = args.get('max_len')
        self.NUM_WORKERS = args.get('num_workers')
        with open(os.path.join(self.ABSOLUTE_PATH, 'config/vocab.json'), 'r', encoding='utf-8') as f:
            self.VOCABS = json.load(f)
        self.VOCAB_NUM = len(self.VOCABS)

        PredictConfig.model_config(self.MODEL_METHOD)


class MultiPredictConfig(BasicConfig):
    """
    预测不设置使用哪个GPU，默认运行在只在1个默认的GPU上
    """
    def __init__(self, args):
        super(MultiPredictConfig, self).__init__()
        self.MODEL_FOLD = os.path.join(self.CHECKPOINT_FOLD, args.model_fold)
        with open(os.path.join(self.MODEL_FOLD, 'config.json'), 'r', encoding='utf-8') as f:
            config = json.load(f)

        self.BATCH_SIZE = args.batch_size
        self.HALF = args.half
        self.PRETRAINED = os.path.join(self.ABSOLUTE_PATH, 'vectors.txt')
        self.MODEL_METHOD = config.get('model_method')

        with open(os.path.join(self.MODEL_FOLD, 'label.json'), 'r', encoding='utf-8') as f:
            self.LABELS = json.load(f)
        
        self.CLASS_NUM = len(self.LABELS)
        self.N_GRAM = config.get('n_gram')
        self.VOCAB_MAX_NUM = config.get('vocab_max_num')
        self.PAD_DIRECTION = config.get('pad_direction')

        self.LOG_LEVEL = config.get('log_level')
        self.LOG_FILE = None
        self.IS_EMBEDDING = config.get('is_embedding')
        self.EMBEDDING_DIM = config.get('embedding_dim')
        self.MAX_LEN = config.get('max_len')
        self.NUM_WORKERS = config.get('num_workers')
        with open(os.path.join(self.MODEL_FOLD, 'vocab.json'), 'r', encoding='utf-8') as f:
            self.VOCABS = json.load(f)
        self.VOCAB_NUM = len(self.VOCABS)

        MultiPredictConfig.model_config(self.MODEL_METHOD)
