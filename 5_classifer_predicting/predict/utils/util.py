import copy
import os
import sys
import time
import unicodedata
import traceback
from ctypes import POINTER, Structure, c_char, c_char_p, cdll
from multiprocessing import Pool

import numpy as np
import pandas as pd


def import_class(import_str):
    mod_str, _sep, class_str = import_str.rpartition('.')
    __import__(mod_str)
    try:
        return getattr(sys.modules[mod_str], class_str)
    except AttributeError:
        raise ImportError(
            'Class %s cannot be found (%s)' %
            (class_str, traceback.format_exception(*sys.exc_info())))


def import_object(import_str, *args, **kwargs):
    return import_class(import_str)(*args, **kwargs)


def import_fun(import_str):
    return import_class(import_str)


def import_module(import_str):
    __import__(import_str)
    return sys.modules[import_str]


class Singleton(object):
    '''单例模式装饰器
    '''

    def __init__(self, cls):
        self._cls = cls
        self._instance = {}

    def __call__(self):
        if self._cls not in self._instance:
            self._instance[self._cls] = self._cls()
        return self._instance[self._cls]


def wrap(
        wcounter,
        fn,
        p1,
        p2,
):
    return wcounter._read(fn, p1, p2)


def tokenizer(sent, label=None):
    _int = []
    sent_ = []
    if not isinstance(sent, list):
        sent = list(sent)
    if label is None:
        label = [''] * len(sent)
    _label = ''
    labels = []
    for char, label_ in zip(sent, label):
        if char.isdigit():
            if _int == []:
                _label = label_
            _int.append(char)
        elif char in [',', '.'] and _int != []:
            _int.append(char)
        else:
            if _int != []:
                sent_.append(''.join(_int))
                labels.append(_label)
                sent_.append(char)
                labels.append(label_)
                _int = []
                _label = ''
            else:
                sent_.append(char)
                labels.append(label_)
    return sent_, labels


def detokenizer(sent, label=None, mode='crf'):
    new_sent = []
    new_label = []
    if mode not in ['bert', 'distilling_bert']:
        _int = []
        if label is None:
            label = [''] * len(sent)
        _label = ''
        for char, label_ in zip(sent, label):
            if len(char) > 1:
                new_sent.extend(list(char))
                new_label.append(label_)
                label_ = label_.split('-')
                if len(label_) > 1:
                    __label = ['I-' + label_[1]] * (len(char) - 1)
                    if len(__label) > 0:
                        new_label.extend(__label)
                else:
                    new_label.extend(label_ * (len(char) - 1))
            else:
                new_sent.append(char)
                new_label.append(label_)
    else:
        new_label = []
        new_sent = []
        for word,label in zip(sent,label):
            if not word.startswith('##'):
                new_sent.append(word)
                new_label.append(label)
            else:
                print(new_sent[-1])
                print(''.join(word[2:]))
                new_sent[-1]= new_sent[-1] + word[2:]
    return new_sent, new_label


class Predict_Data():
    def __init__(self, file_path, sep=None, chunknum=409600, log=None,hdfs_client=None):
        self.file_path = file_path
        self.sep = sep
        self.chunknum = chunknum
        self.log = log
        self.hdfs_client = hdfs_client
        if self.hdfs_client is not None:
            self.rd = self.hdfs_client.read_generator(file_path, self.chunknum,sep=self.sep)
        else:
            self.rd = self._read_line()

    def __iter__(self):
        return self

    def __next__(self):
        start = time.time()
        data,data_len = next(self.rd)
        cost = '读取 {0} 条数据，耗时：{1:.3f} 秒'.format(data_len, time.time() - start)
        if self.log is not None:
            self.log.info(cost)
        else:
            print(cost)
        return data

    def _read_line(self):
        data = []
        with open(self.file_path,mode='r', encoding='utf-8',errors='ignore') as f:
            for i, line in enumerate(f):
                if self.sep is not None:
                    data.append(line.rstrip('\n').split(self.sep))
                    if (i + 1) % self.chunknum == 0:
                        yield data,len(data)
                        data = []
                else:
                    data.append(line)
                    if (i + 1) % self.chunknum == 0:
                        yield ''.join(data),len(data)
                        data = []
            if self.sep is not None:
                yield data,len(data)
            else:
                yield ''.join(data),len(data)


def get_words_targs_bios(string, label):
    word = [0, None]
    words = []
    targs = []
    ST = False
    for i, k in enumerate(label):
        if k in ['[SEP]', '<PAD>', '<bos>', '<eos>', '<pad>']:
            k = 'O'
        if k != 'O':
            k = k.split('-')
            if k[0] == 'B':
                if ST == True:
                    words.append(''.join(string[word[0]:i]))
                    targs.append(word[1])
                    word[0] = i
                    word[1] = k[1]
                else:
                    ST = True
                    word[0] = i
                    word[1] = k[1]
            if k[0] == 'I':
                if ST:
                    if k[1] != word[1]:
                        words.append(''.join(string[word[0]:i]))
                        targs.append(word[1])
                        ST = True
                        word[0] = i
                        word[1] = k[1]
                else:
                    ST = True
                    word[0] = i
                    word[1] = k[1]
        elif ST:
            words.append(''.join(string[word[0]:i]))
            targs.append(word[1])
            ST = False
    if ST:
        words.append(''.join(string[word[0]:i + 1]))
        targs.append(word[1])
    return words, targs





def get_entities(seq,sent, suffix=False):
    """Gets entities from sequence.

    Args:
        seq (list): sequence of labels.

    Returns:
        list: list of (chunk_type, chunk_start, chunk_end).

    Example:
        >>> from seqeval.metrics.sequence_labeling import get_entities
        >>> seq = ['B-PER', 'I-PER', 'O', 'B-LOC']
        >>> get_entities(seq)
        [('PER', 0, 1), ('LOC', 3, 3)]
    """
    # for nested list
    if any(isinstance(s, list) for s in seq):
        seq = [item for sublist in seq for item in sublist + ['O']]

    prev_tag = 'O'
    prev_type = ''
    begin_offset = 0
    chunks = []
    for i, chunk in enumerate(seq + ['O']):
        if chunk in ['[SEP]', '<PAD>', '<bos>', '<eos>', '<pad>', '[CLS]']:
            chunk = 'O'
        if suffix:
            tag = chunk[-1]
            type_ = chunk.split('-')[0]
        else:
            tag = chunk[0]
            type_ = chunk.split('-')[-1]
        if end_of_chunk(prev_tag, tag, prev_type, type_):
            chunks.append((prev_type, ''.join(sent[begin_offset:i]),str(begin_offset)))
        if start_of_chunk(prev_tag, tag, prev_type, type_):
            begin_offset = i
        prev_tag = tag
        prev_type = type_

    return chunks


def end_of_chunk(prev_tag, tag, prev_type, type_):
    """Checks if a chunk ended between the previous and current word.

    Args:
        prev_tag: previous chunk tag.
        tag: current chunk tag.
        prev_type: previous type.
        type_: current type.

    Returns:
        chunk_end: boolean.
    """
    chunk_end = False

    if prev_tag == 'E':
        chunk_end = True
    if prev_tag == 'S':
        chunk_end = True

    if prev_tag == 'B' and tag == 'B':
        chunk_end = True
    if prev_tag == 'B' and tag == 'S':
        chunk_end = True
    if prev_tag == 'B' and tag == 'O':
        chunk_end = True
    if prev_tag == 'I' and tag == 'B':
        chunk_end = True
    if prev_tag == 'I' and tag == 'S':
        chunk_end = True
    if prev_tag == 'I' and tag == 'O':
        chunk_end = True

    if prev_tag != 'O' and prev_tag != '.' and prev_type != type_:
        chunk_end = True

    return chunk_end


def start_of_chunk(prev_tag, tag, prev_type, type_):
    """Checks if a chunk started between the previous and current word.

    Args:
        prev_tag: previous chunk tag.
        tag: current chunk tag.
        prev_type: previous type.
        type_: current type.

    Returns:
        chunk_start: boolean.
    """
    chunk_start = False

    if tag == 'B':
        chunk_start = True
    if tag == 'S':
        chunk_start = True

    if prev_tag == 'E' and tag == 'E':
        chunk_start = True
    if prev_tag == 'E' and tag == 'I':
        chunk_start = True
    if prev_tag == 'S' and tag == 'E':
        chunk_start = True
    if prev_tag == 'S' and tag == 'I':
        chunk_start = True
    if prev_tag == 'O' and tag == 'E':
        chunk_start = True
    if prev_tag == 'O' and tag == 'I':
        chunk_start = True

    if tag != 'O' and tag != '.' and prev_type != type_:
        chunk_start = True

    return chunk_start

def wap(sl, func):
    _data, label, string, length = sl
    tagvocab, tag, mode = func
    _out = copy.deepcopy(_data)
    _out_data = [''] * len(tag)
    if length is not None:
        string = string[1:length - 1]
        label = label[1:length - 1]
    else:
        string = string[1:-1]
        label = label[1:-1]
    label = [tagvocab[_l] for _l in label]

    if mode in ['bert', 'distilling_bert']:
        string, label = detokenizer(string, label,mode)

    tag_word = get_entities(label,string)
    for tw in tag_word:
        n = tw[2]+'$@$'+tw[1]
        if tw[0] in tag:
            if _out_data[tag[tw[0]]] != '':
                _out_data[tag[tw[0]]] += '#ALGO_ITEM_SEP#' + n
            else:
                _out_data[tag[tw[0]]] = n
    _out.extend(_out_data)
    return _out


def wap_output(sf, func):
    data_, pre_,length = sf
    tagvocab, mode = func
    if length is not None:
        string = data_[1:length-1]
        pre_ = pre_[1:length-1]
    else:
        string = data_[1:-1]
        pre_ = pre_[1:-1]
    pre_ = tagvocab.to_tokens(pre_)

    string_n, pre = detokenizer(string, pre_, mode=mode)
    _out = []
    for s,p in zip(string_n, pre):
        _out.append('\x01'.join([s, p]))
    _out.append('\n')
    return _out


def wap_eva_output(sf, func):
    data_, pre_, label_,length = sf
    tagvocab, mode = func
    if length is not None:
        string = data_[1:length]
        pre_ = pre_[1:length]
        label_ = label_[1:-1]
    else:
        string = data_[1:-1]
        pre_ = pre_[1:-1]
        label_ = label_[1:-1]
    pre_ = tagvocab.to_tokens(pre_)

    if pre_ == label_:
        return []
    else:
        string_n, pre = detokenizer(string, pre_,mode=mode)
        _, label = detokenizer(string, label_,mode=mode)
        _out = []
        for s, l, o in zip(string_n, pre, label):
            _out.append('\x01'.join([s, l, o, str(l == o)]))
        _out.append('\n')
        return _out


def wap_c(sl, func):
    _data, label = sl
    tagvocab, tag, msg_num, tokenizer = func
    gt = get_Get_Tag()
    _out = copy.deepcopy(_data)
    string, _ = tokenizer(_data[msg_num])
    label = [tagvocab[_l] for _l in label[1:len(string) + 1]]
    _out_data, _ = gt(string, label, tag)
    _out.extend(_out_data)
    return _out


class StructPointer(Structure):
    _fields_ = [("word", c_char * 1024), ("tag", c_char * 1024)]


@Singleton
class Get_Tag(object):
    def __init__(self, so_paht='./utils/libGetWordTag.so'):
        self.so = cdll.LoadLibrary(so_paht)
        self.func = self.so.get_words_targs_bios
        self.func.restype = POINTER(StructPointer)

    def __call__(self, string, label, tag):
        num = len(string)
        string = (c_char_p *
                  num)(*[c_char_p(bytes(a_, 'utf-8')) for a_ in string])
        label = (c_char_p *
                 num)(*[c_char_p(bytes(a_, 'utf-8')) for a_ in label])
        _out = self.func(string, label, num)
        out = [''] * len(tag)
        for i in range(len(tag)):
            if _out[i].word.decode('utf-8') == 'E':
                break
            out[tag[_out[i].tag.decode('utf-8')]] = _out[i].word.decode(
                'utf-8')
        self.so.freeme(_out)
        return out, i - 1


def get_Get_Tag():
    return Get_Tag()


class BERTBasicTokenizer:
    r"""Runs basic tokenization

    performs invalid character removal (e.g. control chars) and whitespace.
    tokenize CJK chars.
    splits punctuation on a piece of text.
    strips accents and convert to lower case.(If lower is true)

    Parameters
    ----------
    lower : bool, default True
        whether the text strips accents and convert to lower case.

    Examples
    --------
    >>> tokenizer = gluonnlp.data.BERTBasicTokenizer(lower=True)
    >>> tokenizer(' \tHeLLo!how  \n Are yoU?  ')
    ['hello', '!', 'how', 'are', 'you', '?']
    >>> tokenizer = gluonnlp.data.BERTBasicTokenizer(lower=False)
    >>> tokenizer(' \tHeLLo!how  \n Are yoU?  ')
    ['HeLLo', '!', 'how', 'Are', 'yoU', '?']

    """

    def __init__(self, lower=True):
        self.lower = lower

    def __call__(self, sample):
        """

        Parameters
        ----------
        sample:  str (unicode for Python 2)
            The string to tokenize. Must be unicode.

        Returns
        -------
        ret : list of strs
            List of tokens
        """
        return self._tokenize(sample)

    def _tokenize(self, text):
        """Tokenizes a piece of text."""
        text = self._clean_text(text)

        # This was added on November 1st, 2018 for the multilingual and Chinese
        # models. This is also applied to the English models now, but it doesn't
        # matter since the English models were not trained on any Chinese data
        # and generally don't have any Chinese data in them (there are Chinese
        # characters in the vocabulary because Wikipedia does have some Chinese
        # words in the English Wikipedia.).
        text = self._tokenize_chinese_chars(text)
        orig_tokens = self._whitespace_tokenize(text)
        split_tokens = []
        for token in orig_tokens:
            if self.lower:
                token = token.lower()
                token = self._run_strip_accents(token)
            split_tokens.extend(self._run_split_on_punc(token))

        output_tokens = self._whitespace_tokenize(' '.join(split_tokens))
        return output_tokens

    def _clean_text(self, text):
        """Performs invalid character removal and whitespace cleanup on text."""
        output = []
        for char in text:
            cp = ord(char)
            if cp in (0, 0xfffd) or self._is_control(char):
                continue
            if self._is_whitespace(char):
                output.append(' ')
            else:
                output.append(char)
        return ''.join(output)

    def _is_control(self, char):
        """Checks whether `chars` is a control character."""
        # These are technically control characters but we count them as whitespace
        # characters.
        if char in ['\t', '\n', '\r']:
            return False
        cat = unicodedata.category(char)
        if cat.startswith('C'):
            return True
        return False

    def _tokenize_chinese_chars(self, text):
        """Adds whitespace around any CJK character."""
        output = []
        for char in text:
            cp = ord(char)
            if self._is_chinese_char(cp):
                output.append(' ')
                output.append(char)
                output.append(' ')
            else:
                output.append(char)
        return ''.join(output)

    def _is_chinese_char(self, cp):
        """Checks whether CP is the codepoint of a CJK character."""
        # This defines a "chinese character" as anything in the CJK Unicode block:
        #   https://en.wikipedia.org/wiki/CJK_Unified_Ideographs_(Unicode_block)
        #
        # Note that the CJK Unicode block is NOT all Japanese and Korean characters,
        # despite its name. The modern Korean Hangul alphabet is a different block,
        # as is Japanese Hiragana and Katakana. Those alphabets are used to write
        # space-separated words, so they are not treated specially and handled
        # like the all of the other languages.
        if ((0x4E00 <= cp <= 0x9FFF) or (0x3400 <= cp <= 0x4DBF)
                or (0x20000 <= cp <= 0x2A6DF) or (0x2A700 <= cp <= 0x2B73F)
                or (0x2B740 <= cp <= 0x2B81F) or (0x2B820 <= cp <= 0x2CEAF)
                or (0xF900 <= cp <= 0xFAFF) or (0x2F800 <= cp <= 0x2FA1F)):
            return True

        return False

    def _run_strip_accents(self, text):
        """Strips accents from a piece of text."""
        text = unicodedata.normalize('NFD', text)
        output = []
        for char in text:
            cat = unicodedata.category(char)
            if cat == 'Mn':
                continue
            output.append(char)
        return ''.join(output)

    def _run_split_on_punc(self, text):
        """Splits punctuation on a piece of text."""
        chars = list(text)
        i = 0
        start_new_word = True
        output = []
        while i < len(chars):
            char = chars[i]
            if self._is_punctuation(char):
                output.append([char])
                start_new_word = True
            else:
                if start_new_word:
                    output.append([])
                start_new_word = False
                output[-1].append(char)
            i += 1

        return [''.join(x) for x in output]

    def _is_punctuation(self, char):
        """Checks whether `chars` is a punctuation character."""
        cp = ord(char)
        # We treat all non-letter/number ASCII as punctuation.
        # Characters such as "^", "$", and "`" are not in the Unicode
        # Punctuation class but we treat them as punctuation anyways, for
        # consistency.
        group0 = 33 <= cp <= 47
        group1 = 58 <= cp <= 64
        group2 = 91 <= cp <= 96
        group3 = 123 <= cp <= 126
        if (group0 or group1 or group2 or group3):
            return True
        cat = unicodedata.category(char)
        if cat.startswith('P'):
            return True
        return False

    def _is_whitespace(self, char):
        """Checks whether `chars` is a whitespace character."""
        # \t, \n, and \r are technically contorl characters but we treat them
        # as whitespace since they are generally considered as such.
        if char in [' ', '\t', '\n', '\r']:
            return True
        cat = unicodedata.category(char)
        if cat == 'Zs':
            return True
        return False

    def _whitespace_tokenize(self, text):
        """Runs basic whitespace cleaning and splitting on a piece of text."""
        text = text.strip()
        tokens = text.split()
        return tokens


class BERTTokenizer:

    _special_prefix = u'##'

    def __init__(self,
                 token_list,
                 unknown_token='<unk>',
                 lower=True,
                 max_input_chars_per_word=200):
        self.unknown_token = unknown_token
        self.token_list = token_list
        self.max_input_chars_per_word = max_input_chars_per_word
        self.basic_tokenizer = BERTBasicTokenizer(lower=lower)

    def __call__(self, sample):

        return self._tokenizer(sample)

    def _tokenizer(self, text):
        split_tokens = []
        for token in self.basic_tokenizer(text):
            for sub_token in self._tokenize_wordpiece(token):
                split_tokens.append(sub_token)

        return split_tokens

    def _tokenize_wordpiece(self, text):

        output_tokens = []
        for token in self.basic_tokenizer._whitespace_tokenize(text):
            chars = list(token)
            if len(chars) > self.max_input_chars_per_word:
                output_tokens.append(self.unknown_token)
                continue
            is_bad = False
            start = 0
            sub_tokens = []
            while start < len(chars):
                end = len(chars)
                cur_substr = None
                while start < end:
                    substr = ''.join(chars[start:end])
                    if start > 0:
                        substr = self._special_prefix + substr
                    if substr in self.token_list:
                        cur_substr = substr
                        break
                    end -= 1
                if cur_substr is None:
                    is_bad = True
                    break
                sub_tokens.append(cur_substr)
                start = end
            if is_bad:
                output_tokens.append(self.unknown_token)
            else:
                output_tokens.extend(sub_tokens)
        return output_tokens


def pack(msg, data_id, data):
    msg_b = msg.encode()
    data_id_b = data_id.encode()
    msg_len = len(msg_b)
    data_id_len = len(data_id_b)
    new_data = chr(msg_len).encode() + chr(data_id_len).encode() + msg_b + data_id_b + data
    return new_data


def unpack(data):
    data = memoryview(data)
    msg_len = data[0]
    data_id_len = data[1]
    msg = data[2:msg_len + 2].tobytes().decode()
    data_id = data[msg_len+2:data_id_len+msg_len+2].tobytes().decode()
    new_data = data[msg_len+data_id_len + 2:]
    return msg, data_id, new_data
