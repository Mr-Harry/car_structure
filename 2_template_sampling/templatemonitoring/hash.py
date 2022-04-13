from collections import defaultdict
import hashlib
import struct
import time
import re
import numpy as np
from utils import Tokenizer, Trie


def shingling(text, size=3):
    """
    按指定的shingle size切分文本，可以理解为对传入文本生成n-gram的特征集
    比如:
    input -> '去重算法实现'
    output -> set('去重算', '重算法', '算法实', '法实现')
    :param text: str 传入文本
    :param size: int shingle size大小
    :return: set() 文本特征集合
    """
    text = ''.join(text)
    return list([text[i: i + size] for i in range(len(text) - size + 1)])


def _hash_func(x):
    """
    通过 'Hash Once, Then XOR For Each Additional Value' 来模拟不同的Hash Functions
    :param x:
        bytes: 被哈希的文本
    :return:
        int: 哈希值
    """
    return struct.unpack('<Q', hashlib.sha1(x).digest()[:8])[0]


class MinHash:
    def __init__(self, permutations, bands, seed=1201, hash_func=_hash_func):
        self.seed = seed
        self.permutations = permutations
        self.max_hash = (1 << 64) - 1
        self.mersenne_prime = (1 << 61) - 1
        self.hash_func = hash_func
        self.rand_ints = self._get_randint()
        self.bands = bands
        self.band_size = self.permutations // self.bands
        self.hash_ranges = [(i * self.band_size, (i + 1) * self.band_size) for i in range(self.bands)]
        self.hash_tables = [defaultdict(set) for _ in range(self.bands)]

    def hashing(self, features):
        signature = self._hashing(features)
        meta_hash = self._banding(signature)
        return meta_hash

    def _get_randint(self):
        generator = np.random.RandomState(self.seed)
        rand_ints = np.array([generator.randint(0, self.mersenne_prime, dtype=np.uint64)
                              for _ in range(self.permutations)], dtype=np.uint64)
        return rand_ints

    def _hashing(self, features):
        """
        Generate minhash given features
        :return: np.array(np.uint64()) same size with permutations
        """
        signature = np.ones(self.permutations, dtype=np.uint64) * self.max_hash
        for f in features:
            if type(f) != bytes:
                f = f.encode('utf-8')
            h = self.hash_func(f)
            hs = np.bitwise_and(h, self.rand_ints)
            signature = np.minimum(hs, signature)
        return signature

    def _banding(self, signature):
        band_hashes = np.array([signature[start:end] for start, end in self.hash_ranges])
        meta_hash = [self.hash_func(b_hash) for b_hash in band_hashes]
        return meta_hash


'''
class UnionFind:
    def __init__(self, hash_tables):
        self.hash2id = hash_tables
        # for key in self.hash2id[0]:
        #     for i in self.hash2id[0][key]:
        #         print(i['row_key'])
        #         break
        #     break

    def clustering(self):
        clustered = set()
        parents = dict()
        for bucket in self.hash2id:
            for _, keys in bucket.items():
                keys = list(keys)
                if len(keys) == 1:
                    self.add(keys.pop(), parents)
                else:
                    key_pairs = [(keys[i], keys[i + 1]) for i in range(len(keys) - 1)]
                    for key1, key2 in key_pairs:
                        if key1 not in clustered and key2 not in clustered:
                            self.add(key1, parents)
                            self.add(key2, parents)
                            self.union(key1, key2, parents)
        return self.find_sets(parents)

    def output(self):
        pass

    @classmethod
    def add(cls, x, parrents):
        if x not in parrents:
            parrents[x] = x

    @classmethod
    def find(cls, x, parents):
        while parents.get(x) != x:
            parents[x] = parents[parents[x]]
            x = parents[x]
        return x

    @classmethod
    def union(cls, a, b, parents):
        root_a = cls.find(a, parents)
        root_b = cls.find(b, parents)
        if root_b != root_a:
            parents[root_a] = root_b
            if root_b not in parents:
                parents[root_b] = root_b

    @classmethod
    def find_sets(cls, parents):
        sets = defaultdict(list)
        for child in parents:
            root = cls.find(child, parents)
            sets[root].append(child)
        return list(sets.values())
'''


class UnionFind:

    @classmethod
    def clustering(cls, connected_nodes, parents, clustered):
        for node1, node2 in connected_nodes:
            if node1 not in clustered:
                cls.add(node1, parents)
                clustered.add(node1)
            if node2 not in clustered:
                cls.add(node2, parents)
                clustered.add(node2)
            cls.union(node1, node2, parents)
        return cls.find_sets(parents)

    def output(self):
        pass

    @classmethod
    def add(cls, x, parents):
        if x not in parents:
            parents[x] = x

    @classmethod
    def find(cls, x, parents):
        while parents.get(x) != x:
            parents[x] = parents[parents[x]]
            x = parents[x]
        return x

    @classmethod
    def union(cls, a, b, parents):
        root_a = cls.find(a, parents)
        root_b = cls.find(b, parents)
        if root_b != root_a:
            parents[root_a] = root_b
            if root_b not in parents:
                parents[root_b] = root_b

    @classmethod
    def find_sets(cls, parents):
        sets = defaultdict(list)
        for child in parents:
            root = cls.find(child, parents)
            sets[root].append(child)
        return list(sets.values())

