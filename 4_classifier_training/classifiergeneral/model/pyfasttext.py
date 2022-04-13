# -*- coding: utf-8 -*-
import torch
from torch import nn
from .BasicModule import BasicModule


class FASTTEXT(BasicModule):
    def __init__(self, config):
        super(FASTTEXT, self).__init__()
        self.embedding = nn.Embedding(config.VOCAB_NUM, config.EMBEDDING_DIM, padding_idx=0)
        self.bn = nn.BatchNorm1d(config.EMBEDDING_DIM)
        self.fc = nn.Linear(config.EMBEDDING_DIM, config.CLASS_NUM)

    def forward(self, x):
        x = self.embedding(x)
        x = torch.mean(x, dim=1)
        x = self.bn(x)
        outputs = self.fc(x)
        return outputs
