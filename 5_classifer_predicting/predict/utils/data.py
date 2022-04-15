import itertools
import os
import random
import time
from multiprocessing import Pool
from utils.sequence_labeling import get_entities

import gluonnlp as nlp
import numpy as np
import pandas as pd
import os
from collections import Counter
from mxnet.gluon.data import ArrayDataset


class Vocab_NUM(object):
    def __init__(self, vocab):
        self.vocab = vocab

    def to_indices(self, sent):
        sent_ = []
        for s in sent:
            if s in [self.vocab.bos_token, self.vocab.eos_token]:
                sent_.append(self.vocab[s])
            elif len(s) > 1:
                sent_.append(self.vocab['<NUM>'])
            elif ('\u0041' <= s <= '\u005a') or ('\u0061' <= s <= '\u007a'):
                sent_.append(self.vocab['<ENG>'])
            else:
                sent_.append(self.vocab[s])
        return sent_

    def __len__(self):
        return len(self.vocab)


class NERData(ArrayDataset):
    def __init__(self, file_path, is_fewerentities=False, tokenizer=None, is_bert=False):
        self.file_path = file_path
        self.tokenizer = tokenizer
        self.is_fewerentities = is_fewerentities
        self.is_bert = is_bert
        super(NERData, self).__init__(self._read_data())

    def _read(self, file_path):
        data = []
        examples_len = []
        tagset = set()
        if self.is_bert:
            tagset.update(["X", "[CLS]", "[SEP]"])
        try:
            with open(file_path, encoding='utf-8') as f:
                lines = f.readlines()
                if self.is_bert:
                    sent_, tag_ = [], []
                else:
                    sent_, tag_ = ['<eos>'], ['<eos>']

                for line in lines:
                    if line != '\n':
                        res = line.strip('\n').split('\t')
                        char = res[0]
                        label = res[1]
                        if label[0] not in ['B', 'I', 'O']:
                            if label[0] == 'S':
                                label = 'B'+label[1:]
                            else:
                                label = 'I'+label[1:]
                        tagset.update([label])
                        sent_.append(char)
                        tag_.append(label)
                    else:
                        if len(sent_) != 0:
                            if not self.is_bert:
                                sent_.append('<bos>')
                                tag_.append('<bos>')
                                if self.tokenizer is not None:
                                    sent_, tag_ = self.tokenizer(sent_, tag_)
                            data.append((sent_, tag_))
                            examples_len.append(len(sent_))
                            if self.is_bert:
                                sent_, tag_ = [], []
                            else:
                                sent_, tag_ = ['<eos>'], ['<eos>']
        except Exception as e:
            print(e)
            print(file_path)
            return [], [], {}
        print(file_path, len(data))
        return data, examples_len, tagset

    def _read_data(self):
        data = []
        examples_len = []
        tagset = set()
        if os.path.isdir(self.file_path):
            for file_path in os.listdir(self.file_path):
                file_path = os.path.join(self.file_path, file_path)
                _data, _examples_len, _tagset = self._read(file_path)
                data.extend(_data)
                examples_len.extend(_examples_len)
                tagset.update(_tagset)
            self.data_len = examples_len
            self.tag = tagset
            return data
        else:
            _data, _examples_len, _tagset = self._read(self.file_path)
            self.data_len = _examples_len
            self.tag = _tagset
            return _data

    def shuffle(self):
        random.shuffle(self._data[0])

    def get_entitie_dict(self):
        entitie_dict = {}
        for d in self._data[0]:
            en = get_entities(d[1][1:-1])
            for e in en:
                if e[0] not in entitie_dict:
                    entitie_dict[e[0]] = []
                entite = (d[0][e[1] + 1:e[2] + 2], d[1][e[1] + 1:e[2] + 2])
                if entite not in entitie_dict[e[0]]:
                    entitie_dict[e[0]].append(entite)
        return entitie_dict

    def build_vocab(self):
        train_seqs = []
        for sample in self._data[0]:
            ss = []
            for s in sample[0]:
                if len(s) > 1 and s not in ['<bos>', '<eos>']:
                    s = '<NUM>'
                elif ('\u0041' <= s <= '\u005a') or ('\u0061' <= s <= '\u007a'):
                    s = '<ENG>'
                ss.append(s)
            train_seqs.append(ss)

        counter = nlp.data.count_tokens(
            list(itertools.chain.from_iterable(train_seqs)))
        vocab = Vocab_NUM(nlp.Vocab(counter))
        if self.is_fewerentities:
            fewerlist = self.GetFewerEntities()
        else:
            fewerlist = []
        for k in fewerlist:
            if 'B-' + k in self.tag:
                self.tag.remove('B-' + k)
            elif 'I-' + k in self.tag:
                self.tag.remove('I-' + k)
            else:
                print(self.tag, k)
        tagvocab = nlp.Vocab(nlp.data.count_tokens(self.tag))
        return vocab, tagvocab, fewerlist if self.is_fewerentities else None

    def split(self, n=0.3):
        assert isinstance(n, float)
        assert n >= 0 and n <= 1
        self.shuffle()
        num = int(len(self._data[0]) * n)
        valid_data = self._data[0][:num]
        train_data = self._data[0][num:]

        train_len = []
        for d in train_data:
            train_len.append(len(d[0]))
        return ArrayDataset(train_data), train_len, ArrayDataset(valid_data)

    def GetFewerEntities(self):
        o = []
        for d in self._data[0]:
            for dd in d[1]:
                dd = dd.split('-')
                if len(dd) > 1:
                    if dd[0] == 'B':
                        o.append(dd[1])
        o_m = Counter(o)
        sm = sum(list(o_m.values()))
        srk = sorted([[k, o_m[k], (o_m[k]/sm)/((sm/len(o_m.keys()))/sm)]
                      for k in o_m.keys()], key=lambda x: x[1])
        m_list = [x[0] for x in filter(lambda x:x[2] < 0.01, srk)]
        print(self.file_path, m_list)
        return m_list


class Data_Augmentation_NER:
    def __init__(self, entitie_dict, pro=.5):
        self.entitie_dict = entitie_dict
        self.pro = pro

    def augment(self, data):
        if self.pro > random.random():
            ge = get_entities(data[1][1:-1])
            if len(ge) > 0:
                ges = random.choice(ge)
                if len(self.entitie_dict[ges[0]]) > 0:
                    string_start = data[0][0:ges[1]+1]
                    string_end = data[0][ges[2]+2:]
                    label_start = data[1][0:ges[1]+1]
                    label_end = data[1][ges[2]+2:]

                    ns = random.choice(self.entitie_dict[ges[0]])
                    n_string = []
                    n_label = []
                    n_string.extend(string_start)
                    n_string.extend(ns[0])
                    n_string.extend(string_end)

                    n_label = []
                    n_label.extend(label_start)
                    n_label.extend(ns[1])
                    n_label.extend(label_end)
                    return n_string, n_label
        return data

    def __call__(self, *record):
        return self.augment(record)


class NERTransform(object):
    def __init__(self, vocab, tagvocab, fewerlist=None, augment=None):
        self.vocab = vocab
        self.tagvocab = tagvocab
        self.fewerlist = fewerlist
        self.augment = augment

    def __call__(self, *record):
        if self.augment is not None:
            record = self.augment(*record)
        data_ = record[0]
        if self.fewerlist is not None:
            label_ = []
            for l in record[1]:
                ll = l.split('-')
                if len(ll) > 1 and ll[1] in self.fewerlist:
                    l = 'O'
                label_.append(l)
        else:
            label_ = record[1]
        data_ids = self.vocab.to_indices(data_)
        label_ids = self.tagvocab.to_indices(label_)
        return data_ids, [0], len(data_ids), label_ids, data_, label_


class BERTforNERTransform(object):
    def __init__(self,
                 tokenizer,
                 vocab,
                 label_vocab,
                 max_seq_length=128,
                 fewerlist=None,
                 augment=None):
        self.tokenizer = tokenizer
        self.vocab = vocab
        self.label_vocab = label_vocab
        self.fewerlist = fewerlist
        self.augment = augment
        self.max_seq_length = max_seq_length

    def _transform(self, X, Y):
        tokens = []
        labels = []
        tokens.append('[CLS]')
        labels.append('[CLS]')

        for i, word in enumerate(X):
            token = self.tokenizer(word)
            tokens.extend(token)
            label_1 = Y[i]
            for m in range(len(token)):
                if m == 0:
                    labels.append(label_1)
                else:
                    labels.append("X")

        if len(tokens) >= self.max_seq_length - 1:
            tokens = tokens[0:(self.max_seq_length - 2)]
            labels = labels[0:(self.max_seq_length - 2)]
        segment_ids = [0] * self.max_seq_length
        label_ids = []

        tokens.append('[SEP]')
        labels.append('[SEP]')

        input_ids = self.vocab.to_indices(tokens)
        label_ids = self.label_vocab.to_indices(labels)
        valid_length = len(input_ids)

        while len(input_ids) < self.max_seq_length:
            input_ids.append(self.vocab['[PAD]'])
            label_ids.append(self.label_vocab['<pad>'])

        assert len(input_ids) == self.max_seq_length
        assert len(segment_ids) == self.max_seq_length
        assert len(label_ids) == self.max_seq_length

        return input_ids, segment_ids, valid_length, label_ids, tokens, labels

    def __call__(self, X, Y=None):
        if not Y:
            Y = ['<pad>'] * len(X)
        if self.fewerlist is not None:
            _Y = []
            for l in Y:
                ll = l.split('-')
                if len(ll) > 1 and ll[1] in self.fewerlist:
                    l = 'O'
                _Y.append(l)
            Y = _Y
        if self.augment is not None:
            X, Y = self.augment(X, Y)
        if len(X) != len(Y):
            print(X, Y)
        input_ids, segment_ids, valid_length, label_ids, tokens, labels = self._transform(
            X, Y)
        
        return input_ids, segment_ids, valid_length, label_ids, tokens, labels


class PredictDataTransform(object):
    def __init__(self, vocab, num, tokenizer=None, mode='crf', max_seq_length=140):
        self.vocab = vocab
        self.num = num
        self.tokenizer = tokenizer
        self.mode = mode
        self.max_seq_length = max_seq_length
        self.pad = nlp.data.PadSequence(self.max_seq_length, pad_val=1)

    def __call__(self, *record):
        if len(record[0]) <= self.num:
            X = ''
        else:
            X = record[0][self.num]
        if self.mode not in ['bert', 'distilling_bert']:
            data_ = ['<eos']
            if self.tokenizer is None:
                _data_ = list(X)
            else:
                _data_, _ = self.tokenizer(X)
            data_.extend(_data_)
            data_ = data_[:self.max_seq_length-1]
            data_.append('<bos>')
            data_ids = self.vocab.to_indices(data_)
            return np.array(self.pad(data_ids), dtype='int32'), 0, len(data_), np.array([0], dtype='int32'), data_, ['0']
        else:
            data = list(X)
            input_ids, segment_ids, valid_length, label_ids, tokens, labels = self.tokenizer(
                data)
            return input_ids, segment_ids, valid_length, label_ids, tokens, labels
