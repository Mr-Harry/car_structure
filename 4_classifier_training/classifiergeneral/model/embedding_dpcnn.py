# -*- coding: utf-8 -*-
import torch
import torch.nn as nn


class DPCNN(nn.Module):
    def __init__(self, config):
        super(DPCNN, self).__init__()
        self.config = config
        self.channel_size = self.config.CHANNEL_SIZE
        if self.config.IS_EMBEDDING:
            # self.embedding = nn.Embedding(self.config.VOCAB_NUM, self.config.EMBEDDING_DIM)
            self.embedding = nn.Embedding.from_pretrained()
        self.conv_region_embedding = nn.Conv2d(1, self.channel_size, (3, self.config.EMBEDDING_DIM), stride=1)
        self.pad_conv = nn.ZeroPad2d((0, 0, 1, 1))
        self.conv = nn.Conv2d(self.channel_size, self.channel_size, (3, 1), stride=1)
        self.pad_pool = nn.ZeroPad2d((0, 0, 0, 1))
        self.pooling = nn.MaxPool2d(kernel_size=(3, 1), stride=2)
        self.relu = nn.ReLU()
        self.fc = nn.Linear(self.config.MIN_BLOCK_SIZE * self.channel_size, self.config.CLASS_NUM)

    def forward(self, x):
        if self.config.IS_EMBEDDING:
            x = self.embedding(x)  # [batch_size, length, embedding_dimension]
        batch_size, length, embed_dim = x.shape
        x = x.view((batch_size, 1, length, embed_dim))
        x = self.conv_region_embedding(x)  # [batch_size, channel_size, length]
        x = self.pad_conv(x)
        x = self.relu(x)
        x = self.conv(x)
        x = self.relu(x)
        x = self.conv(x)

        while x.size()[-2] > self.config.MIN_BLOCK_SIZE:
            x = self._block(x)

        x = x.view(batch_size, self.config.MIN_BLOCK_SIZE * self.channel_size)
        x = self.fc(x)

        return x

    def _block(self, x):
        # Pooling
        x = self.pad_pool(x)
        px = self.pooling(x)

        # Convolution
        x = self.pad_conv(px)
        x = self.relu(x)
        x = self.conv(x)

        x = self.pad_conv(x)
        x = self.relu(x)
        x = self.conv(x)

        # Short Cut
        x = x + px

        return x
