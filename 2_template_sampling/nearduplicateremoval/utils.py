import os
import sys
import pickle


class Trie(object):
    """定义基本的Trie树结构，便于存储词典（词+词频）。
    主要的代码量是基于Python类的特殊方法来定义一些函数，
    达到表面上看起来和dict的使用方法基本一致的效果。
    """
    def __init__(self, insert_path=None, pos=False):
        self.dic = {}  # 叶子节点值为{father_char:son_char},尾节点值为{True:word}
        self.end = True
        self.pos = pos
        # 从文件中加载，文件的每一行是 词
        if isinstance(insert_path, str) and os.path.exists(insert_path):
            if insert_path.endswith(".pkl"):
                with open(insert_path, 'rb') as f:
                    self.dic = pickle.load(f)
            else:
                with open(insert_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        if self.pos:
                            line = line.rstrip('\n').split('\t')
                            self.additem(line[0], float(line[1]))
                        else:
                            self.additem(line.rstrip('\n'))

    def additem(self, word, pos=None):
        self.root = self.dic
        for char in word:
            if char not in self.root:
                self.root[char] = {}
            self.root = self.root[char]

        if pos:
            self.root[self.end] = (word, pos)
        else:
            self.root[self.end] = word

    def delitem(self, item):  # 删除某个词
        self.root = self.dic
        for char in item:
            if char not in self.root:
                return None
            self.root = self.root[char]

        if self.end in self.root:
            del self.root[self.end]

    def search(self, sent):  # 返回字符串中所有能找到的词语
        result = {}  # 结果是{(start, end): (词)}的字典
        for i, _ in enumerate(sent):
            self.root = self.dic
            for j, char in enumerate(sent[i:]):
                if char in self.root:
                    self.root = self.root[char]
                    if self.end in self.root:
                        result[i, i + j + 1] = self.root[self.end]
                else:
                    break
        return result

    def save(self, save_path):
        with open(save_path, 'wb') as f:
            pickle.dump(self.dic, f)


class TrieNode(object):
    def __init__(self, value=None, pos=None):
        self.value = value  # 值
        self.pos = pos  # 实体名称
        self.fail = None  # 失败指针
        self.tail = None  # 尾标志，储存单词
        self.word_length = -1
        self.children = {}


class DAG(object):
    """定义一般的有向无环图（Directed Acyclic Graph）对象，
    便于在各种场景下使用。其中optimal_path方法使用viterbi
    算法来给出最优路径。
    """

    def __init__(self, nb_node, null_score=-1000):
        self.edges = {}
        self.nb_node = nb_node
        self.null_score = null_score

    def __setitem__(self, start_end, score):  # 构建图上的加权边
        start, end = start_end  # key是(start, end)下标对
        if start not in self.edges:
            self.edges[start] = {}
        self.edges[start][end] = score

    def optimal_path(self):
        """动态规划求最优路径
        result的key是当前字的下标，代表截止到前一字的规划结果，
        result的第一个值是list，表示匹配片段的(start, end)下标对；
        result的第二个值是路径的分数
        """
        result = {0: ([], 1)}
        start = 0  # 当前字的下标
        length = self.nb_node
        while start < length:
            if start in self.edges:  # 如果匹配得上
                for i, j in self.edges[start].items():  # 这里i是终止下标
                    score = result[start][1] + j  # 当前路径分数
                    # 如果当前路径不在result中，或者它的分数超过已有路径，则更新
                    if i not in result or (score >= result[i][1]):
                        result[i] = result[start][0] + [(start, i)], score

            # 为了下一步的匹配，如果下一字还不在result中，
            # 就按单字来插入，概率为null_score
            if start + 1 not in result:
                score = result[start][1] + self.null_score
                result[start + 1] = result[start][0] + [(start, start + 1)], score

            start += 1

        return result[self.nb_node][0]


class Tokenizer(object):
    def __init__(self, trie):
        self.trie = trie

    def __call__(self, sent, quer_v=None):
        dag = DAG(len(sent))
        ts = self.trie.search(sent)
        print(ts)
        for i in ts.keys():
            dag[i] = ts[i][-1]

        dags = dag.optimal_path()
        res = [ts[key][0] for key in dags if key in ts]
        return res


class Shingler(object):
    def __init__(self, size=3):
        self.size = size

    def __call__(self, text):
        """
        按指定的shingle size切分文本，可以理解为对传入文本生成n-gram的特征集
        比如:
        input -> '去重算法实现'
        output -> set('去重算', '重算法', '算法实', '法实现')
        :param text: str 传入文本
        :return: set() 文本特征集合
        """
        text = ''.join(text)
        return list([text[i: i + self.size] for i in range(len(text) - self.size + 1)])

