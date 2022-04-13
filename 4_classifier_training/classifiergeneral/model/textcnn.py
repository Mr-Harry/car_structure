# -*- coding: utf-8 -*-
import torch
from torch import nn
from .BasicModule import BasicModule


class TEXTCNN(BasicModule):
    def __init__(self, config):
        super(TEXTCNN, self).__init__()
        self.config = config

        self.embedding = nn.Embedding(self.config.VOCAB_NUM, self.config.EMBEDDING_DIM, padding_idx=0)
        self.convs = nn.ModuleList(nn.Sequential(
            nn.Conv1d(self.config.MAX_LEN, self.config.CHANNEL_SIZE, kernel_size=kernel_size),
            nn.ReLU(),
            nn.MaxPool1d(kernel_size=self.config.EMBEDDING_DIM - kernel_size)
        ) for kernel_size in self.config.KERNEL_SIZE)
        self.bn = nn.BatchNorm1d(len(self.config.KERNEL_SIZE) * self.config.CHANNEL_SIZE)
        self.fc = nn.Linear(len(self.config.KERNEL_SIZE) * self.config.CHANNEL_SIZE, self.config.CLASS_NUM)

    def forward(self, x):
        x = self.embedding(x)
        conv_x = [conv(x).view(-1, self.config.CHANNEL_SIZE) for conv in self.convs]
        x = torch.cat(conv_x, dim=-1)
        x = self.bn(x)
        outputs = self.fc(x)
        return outputs
