# -*- coding: utf-8 -*-
import torch
import torch.nn as nn


class DPCNN(nn.Module):
    def __init__(self, config):
        super(DPCNN, self).__init__()
        self.config = config
        self.channel_size = self.config.CHANNEL_SIZE
        if self.config.IS_EMBEDDING:
            if config.PRETRAINED is not None:
                self.embedding = nn.Embedding.from_pretrained(self.config.PRETRAINED, freeze=False)
            else:
                self.embedding = nn.Embedding(self.config.VOCAB_NUM, self.config.EMBEDDING_DIM)

        self.conv_region_embedding = nn.Sequential(
            nn.Conv1d(self.config.EMBEDDING_DIM, self.channel_size, kernel_size=3, stride=1, padding=1),
            # nn.BatchNorm1d(self.channel_size),
            nn.ReLU(),
            nn.Dropout(p=0.5)
        )

        self.conv = nn.Sequential(
            # nn.BatchNorm1d(self.channel_size),
            nn.ReLU(),
            nn.Conv1d(self.channel_size, self.channel_size, kernel_size=3, stride=1, padding=1),
            # nn.BatchNorm1d(self.channel_size),
            nn.ReLU(),
            nn.Conv1d(self.channel_size, self.channel_size, kernel_size=3, stride=1, padding=1)
        )

        self.pooling = nn.Sequential(
            nn.ConstantPad1d(padding=(0, 1), value=0),
            nn.MaxPool1d(kernel_size=3, stride=2)
        )
        self.conv_block = nn.Sequential(
            # nn.BatchNorm1d(self.channel_size),
            nn.ReLU(),
            nn.Conv1d(self.channel_size, self.channel_size, kernel_size=3, stride=1, padding=1),
            # nn.BatchNorm1d(self.channel_size),
            nn.ReLU(),
            nn.Conv1d(self.channel_size, self.channel_size, kernel_size=3, stride=1, padding=1)
        )

        self.fc = nn.Linear(self.config.MIN_BLOCK_SIZE * self.channel_size, self.config.CLASS_NUM)

    def forward(self, x):
        if self.config.IS_EMBEDDING:
            x = self.embedding(x)  # [batch_size, length, embedding_dimension]
        x = x.permute(0, 2, 1)
        x = self.conv_region_embedding(x)  # [batch_size, channel_size, length]
        x = self.conv(x)

        while x.size()[-1] > self.config.MIN_BLOCK_SIZE:
            x = self._block(x)

        x = x.view(-1, self.config.MIN_BLOCK_SIZE * self.channel_size)
        x = self.fc(x)

        return x

    def _block(self, x):
        # Pooling
        px = self.pooling(x)

        # Convolution
        x = self.conv_block(px)

        # Short Cut
        x = x + px

        return x
