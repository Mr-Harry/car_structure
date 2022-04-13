# coding:utf-8
import os
import re
import random
import math
import json
import numpy as np
from sklearn.utils import shuffle
from sklearn.model_selection import StratifiedKFold
from torch.utils.data import Dataset
from util import Logger
from collections import Counter

EPS = 1e-9


def n_gram(line, num):
    """
    parm:
        line: 原始文本
        num: n-gram的n值
    retutn: 该文本对应的n-gram列表
    """
    if num == 1:
        return line
    else:
        line = re.sub('[^\u4e00-\u9fa5\-]+', ' ', line)
        return [line[i: i+n] for n in range(1, num + 1) for i in range(len(line) - n + 1)]


class DatasetLoader(object):
    def __init__(self, config):
        self.config = config
        self.logger = Logger(config)
        self.label_counts = dict()

        if abs(self.config.TRAIN_RATE + self.config.VALIDATE_RATE + self.config.TEST_RATE - 1.0) >= EPS:
            raise ValueError('sum of TRAIN_RATE, VALIDATE_RATE, TEST_RATE must be 1.0 , but now is {}'.format(self.config.TRAIN_RATE + self.config.VALIDATE_RATE + self.config.TEST_RATE))

        if config.K_FOLD is not None:
            self.trains, self.holdouts = self.data_split()
            for k in range(self.config.K_FOLD):
                # 保存训练数据
                with open(os.path.join(self.config.SAVE_FOLD, 'train_fold_{}.txt'.format(k + 1)), 'w+', encoding='utf-8') as f:
                    outs = [self.config.SEP.join(line) for line in self.trains[k]]
                    print('\n'.join(outs), file=f)
                # 保存验证数据
                with open(os.path.join(self.config.SAVE_FOLD, 'holdout_fold_{}.txt'.format(k + 1)), 'w+', encoding='utf-8') as f:
                    outs = [self.config.SEP.join(line) for line in self.holdouts[k]]
                    print('\n'.join(outs), file=f)

            self.trains = [TextDataset(train, config) for train in self.trains]
            self.holdouts = [TextDataset(holdout, config) for holdout in self.holdouts]

        else:
            self.train, self.validate, self.test = self.data_split()
            # 保存验证数据
            if len(self.validate):
                with open(os.path.join(self.config.SAVE_FOLD, 'validate.txt'), 'w+', encoding='utf-8') as f:
                    outs = [self.config.SEP.join(line) for line in self.validate]
                    print('\n'.join(outs), file=f)
            # 保存训练数据
            if len(self.train):
                with open(os.path.join(self.config.SAVE_FOLD, 'train.txt'), 'w+', encoding='utf-8') as f:
                    outs = [self.config.SEP.join(line) for line in self.train]
                    print('\n'.join(outs), file=f)
            # 保存测试数据
            if len(self.test):
                with open(os.path.join(self.config.SAVE_FOLD, 'test.txt'), 'w+', encoding='utf-8') as f:
                    outs = [self.config.SEP.join(line) for line in self.test]
                    print('\n'.join(outs), file=f)

            self.train = TextDataset(self.train, config)
            self.validate = TextDataset(self.validate, config)
            self.test = TextDataset(self.test, config)

    def data_split(self):
        with open(self.config.TRAIN_DATASET_PATH, 'r', encoding='utf-8') as f:
            lines = [line.rstrip('\n').split(self.config.SEP) for line in f]

        # 分割数据
        train = []
        validate = []
        test = []

        if self.config.args.local_rank == 0: self.logger.info("start split dataset")
        for label in self.config.LABELS:
            select_lines = list(filter(lambda x: x[-1] == label, lines))
            total = len(select_lines)  # 属于该类目的数量
            if self.config.args.local_rank == 0: self.logger.info("{} {}".format(label, total))  # 输出每个类目条数
            if total < 10:
                raise ValueError('the number of {} is less than 10, please add training data'.format(label))
            self.label_counts[label] = total

            train_index = math.ceil(total * self.config.TRAIN_RATE)
            validate_index = math.ceil(total * (self.config.TRAIN_RATE + self.config.VALIDATE_RATE))

            if self.config.TRAIN_RATE:
                if self.config.UNSAMPLE_NUM and (train_index < self.config.UNSAMPLE_NUM):  # 上采样
                    random.seed(self.config.SEED)
                    train += random.choices(select_lines[:train_index], k=self.config.UNSAMPLE_NUM-train_index)
                train += select_lines[:train_index]
            
            if self.config.VALIDATE_RATE:
                validate += select_lines[train_index:validate_index]

            if self.config.TEST_RATE:
                test += select_lines[validate_index:]

        train = shuffle(train, random_state=self.config.SEED)  # 随机排序
        # 创建字典映射
        if self.config.args.local_rank == 0: self.logger.info("start create vocab")

        word_cnt = Counter([word for line in train for word in n_gram(line[0], self.config.N_GRAM)])
        words = [word[0] for word in word_cnt.most_common(self.config.VOCAB_MAX_NUM - 1)]
        for i, word in enumerate(words):
            self.config.VOCABS[word] = i + 2

        self.config.VOCAB_NUM = len(self.config.VOCABS)
        with open(os.path.join(self.config.SAVE_FOLD, 'vocab.json'), 'w+', encoding='utf-8') as f:
            json.dump(self.config.VOCABS, f, ensure_ascii=False)
        if self.config.args.local_rank == 0: self.logger.info("end create vocab")

        if self.config.K_FOLD is not None:
            trains, holdouts = [], []
            skf = StratifiedKFold(n_splits=self.config.K_FOLD, shuffle=True, random_state=self.config.SEED)
            labels = [line[1] for line in train]

            for cv_train_idxs, cv_holdout_idxs in skf.split(range(len(labels)), labels):
                trains.append([train[i] for i in cv_train_idxs])
                holdouts.append([train[i] for i in cv_holdout_idxs])

            if self.config.args.local_rank == 0: self.logger.info("end split dataset")
            return trains, holdouts

        else:
            if self.config.args.local_rank == 0: self.logger.info("len of train is {}, len of validate is {}, len of test is {}".format(len(train), len(validate), len(test)))
            if self.config.args.local_rank == 0: self.logger.info("end split dataset")
            return train, validate, test


class TextDataset(Dataset):
    def __init__(self, data, config, train=True):
        self.data = data
        self.train = train
        self.config = config
        self.label2index = {label : i for i, label in enumerate(self.config.LABELS)}

    def __getitem__(self, index):
        if self.train:
            text = self.data[index][0]
            label = self.data[index][-1]
            x = self.text2vec(text)
            y = self.label2index[label]
            return text, x, y
        else:
            text = self.data[index]
            x = self.text2vec(text)
            return text, x

    def __len__(self):
        return len(self.data)

    def text2vec(self, line):
        line = n_gram(line, self.config.N_GRAM)
        x = np.array([self.config.VOCABS.get(word, self.config.VOCABS['<UNK>']) for word in line[:self.config.MAX_LEN]])
        if self.config.PAD_DIRECTION == 'right':
            x = np.pad(x, (0, self.config.MAX_LEN-len(x)))
        elif self.config.PAD_DIRECTION == 'left':
            x = np.pad(x, (self.config.MAX_LEN-len(x), 0))
        return x.astype(np.int64)
