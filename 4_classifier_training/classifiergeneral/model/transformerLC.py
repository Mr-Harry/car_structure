import math
import torch
from torch import nn
import numpy as np


def padding_mask(query):
    # query 的形状是 [B,L]
    len_q = query.size(1)
    pad_mask = query.eq(0)  # 输出是否等于0的bool型矩阵
    # pad_mask = pad_mask.unsqueeze(1).expand(-1, len_q, -1)
    return pad_mask


class PositionalEmbedding(nn.Module):
    def __init__(self, config, dropout=0.0):
        super(PositionalEmbedding, self).__init__()
        self.dropout = nn.Dropout(p=dropout)

        pe = torch.tensor([
            [pos/np.power(10000, 2.0*(j//2)/config.EMBEDDING_DIM) for j in range(config.EMBEDDING_DIM)]
            for pos in range(config.MAX_LEN)
        ])
        pe[:, 0::2] = torch.sin(pe[:, 0::2])
        pe[:, 1::2] = torch.cos(pe[:, 1::2])
        self.register_buffer('pe', pe)  # [5000, d_model]

    def forward(self, x):
        x = x + self.pe.unsqueeze(0).repeat(x.size(0), 1, 1)
        return self.dropout(x)


class TransformerLC(nn.Module):
    def __init__(self, config):
        super(TransformerLC, self).__init__()
        self.config = config

        if self.config.IS_EMBEDDING:
            self.embedding = nn.Embedding(self.config.VOCAB_NUM, self.config.EMBEDDING_DIM, padding_idx=0)
        
        encoded_layer = nn.TransformerEncoderLayer(d_model=self.config.EMBEDDING_DIM, nhead=self.config.NHEAD)  # 多头的数量
        self.transformer_encoder = nn.TransformerEncoder(encoded_layer, num_layers=self.config.NUM_LAYER)

        self.pos_encoder = PositionalEmbedding(config)
        self.batch_norm = nn.BatchNorm1d(self.config.MAX_LEN * self.config.EMBEDDING_DIM)
        self.linear = nn.Linear(self.config.MAX_LEN * self.config.EMBEDDING_DIM, self.config.CLASS_NUM)

    def forward(self, x):
        mask = padding_mask(x)

        x = self.embedding(x)
        x = self.pos_encoder(x)
        x = x.transpose(0, 1)
        x = self.transformer_encoder(x, src_key_padding_mask=mask)
        x = x.transpose(0, 1)
        x = x.reshape(-1, self.config.MAX_LEN * self.config.EMBEDDING_DIM)
        x = self.batch_norm(x)
        x = self.linear(x)
        return x
