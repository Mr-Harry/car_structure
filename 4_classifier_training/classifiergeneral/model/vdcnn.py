# coding:utf-8
import torch
from torch import nn


# reference : 
# https://github.com/zonetrooper32/VDCNN/blob/keras_version/k_maxpooling.py
# https://www.arxiv-vanity.com/papers/1606.01781/
# The character embedding is of size 16. 
# Training is performed with SGD, 
# using a mini-batch of size 128, 
# an initial learning rate of 0.01 and momentum of 0.9
class IdentityBlock(nn.Module):
    def __init__(self, channels, kernel_size=3, use_bias=True, short_cut=False):
        super(IdentityBlock, self).__init__()
        self.block = nn.Sequential(
            nn.Conv1d(channels, channels, kernel_size, stride=1, padding=1, bias=use_bias),
            nn.BatchNorm1d(channels),
            nn.ReLU(),
            nn.Conv1d(channels, channels, kernel_size, stride=1, padding=1, bias=use_bias),
            nn.BatchNorm1d(channels),
        )
        self.relu = nn.ReLU()
        self.short_cut = short_cut
    
    def forward(self, x):
        out = self.block(x)
        if self.short_cut:
            out = torch.add(x, out)
        return self.relu(out)


class DownSampleBlock(nn.Module):
    def __init__(self, pool_type='max', channels=None):
        super(DownSampleBlock, self).__init__()
        self.pool_type = pool_type
        if self.pool_type == 'max':
            self.pool = nn.MaxPool1d(kernel_size=3, stride=2, padding=1)
        elif self.pool_type == 'conv':
            self.conv = nn.Conv1d(channels, channels, kernel_size=3, stride=2, padding=1)
            self.bn = nn.BatchNorm1d(channels)

    def forward(self, x):
        if self.pool_type == 'max':
            return self.pool(x)
        elif self.pool_type == 'conv':
            x = self.conv(x)
            x = self.bn(x)
            return x
        elif self.pool_type is None:
            return x
        else:
            raise ValueError("unsupported pooling type , must be ['max', 'conv', None] !")


class ConvBlock(nn.Module):
    def __init__(self, channels, kernel_size=3, use_bias=True, short_cut=False, pool_type='max'):
        super(ConvBlock, self).__init__()
        self.short_cut = short_cut
        self.pool_type = pool_type

        self.block = nn.Sequential(
            nn.Conv1d(channels, channels, kernel_size, stride=1, padding=1, bias=use_bias),
            nn.BatchNorm1d(channels),
            nn.ReLU(),
            nn.Conv1d(channels, channels, kernel_size, stride=1, padding=1, bias=use_bias),
            nn.BatchNorm1d(channels)
        )
        self.residual_conv = nn.Sequential(
            nn.Conv1d(channels, channels, kernel_size=1, stride=2),
            nn.BatchNorm1d(channels)
        )
        self.down_sample_block = DownSampleBlock(pool_type, channels)
        self.relu = nn.ReLU()
        if pool_type:
            self.up_conv = nn.Sequential(
                nn.Conv1d(channels, channels * 2, kernel_size=1, stride=1),
                nn.BatchNorm1d(channels * 2)
            )

    def forward(self, x):
        out = self.block(x)
        if self.short_cut:
            residual = self.residual_conv(x)
            out = self.down_sample_block(out)
            out = torch.add(residual, out)
            out = self.relu(out)
        else:
            out = self.relu(out)
            out = self.down_sample_block(out)
        
        if self.pool_type:
            out = self.up_conv(out)
        return out


class KMaxPooling(nn.Module):
    def __init__(self, k):
        super(KMaxPooling, self).__init__()
        self.k = k
    
    def forward(self, x):
        x = torch.topk(x, self.k, dim=2)[0]
        return x


class VDCNN(nn.Module):
    def __init__(self, config):
        super(VDCNN, self).__init__()
        self.config = config

        if self.config.DEPTH == 9:
            self.num_conv_blocks = (1, 1, 1, 1)
        elif self.config.DEPTH == 17:
            self.num_conv_blocks = (2, 2, 2, 2)
        elif self.config.DEPTH == 29:
            self.num_conv_blocks = (5, 5, 2, 2)
        elif self.config.DEPTH == 49:
            self.num_conv_blocks = (8, 8, 5, 3)
        else:
            raise ValueError("unsupported depth for VDCNN, must be [9, 17, 29, 49]")

        self.embedding = nn.Embedding(self.config.VOCAB_NUM, self.config.EMBEDDING_DIM, padding_idx=0)
        self.conv = nn.Conv1d(self.config.EMBEDDING_DIM, 64, kernel_size=3, stride=1, padding=1)
        
        self.identity_block64  = IdentityBlock(64, kernel_size=3, use_bias=True, short_cut=True)
        self.identity_block128 = IdentityBlock(128, kernel_size=3, use_bias=True, short_cut=True)
        self.identity_block256 = IdentityBlock(256, kernel_size=3, use_bias=True, short_cut=True)
        self.identity_block512 = IdentityBlock(512, kernel_size=3, use_bias=True, short_cut=True)

        self.conv_block64 = ConvBlock(64, kernel_size=3, use_bias=True, short_cut=True, pool_type='max')
        self.conv_block128 = ConvBlock(128, kernel_size=3, use_bias=True, short_cut=True, pool_type='max')
        self.conv_block256 = ConvBlock(256, kernel_size=3, use_bias=True, short_cut=True, pool_type='max')
        self.conv_block512 = ConvBlock(512, kernel_size=3, use_bias=True, short_cut=False, pool_type=None)

        self.k_maxpooling = KMaxPooling(8)
        self.fc = nn.Sequential(
            nn.Linear(4096, 2048),
            nn.Linear(2048, 2048),
            nn.Linear(2048, self.config.CLASS_NUM)
        )

    def forward(self, x):
        """
        x : (batch_size, max_len)
        """
        x = self.embedding(x)
        x = x.permute(0, 2, 1)
        x = self.conv(x)

        # Convolutional Block 64
        for _ in range(self.num_conv_blocks[0] - 1):
            x = self.identity_block64(x)
        x = self.conv_block64(x)

        # Convolutional Block 128
        for _ in range(self.num_conv_blocks[1] - 1):
            x = self.identity_block128(x)
        x = self.conv_block128(x)

        # Convolutional Block 256
        for _ in range(self.num_conv_blocks[2] - 1):
            x = self.identity_block256(x)
        x = self.conv_block256(x)

        # Convolutional Block 512
        for _ in range(self.num_conv_blocks[3] - 1):
            x = self.identity_block512(x)
        x = self.conv_block512(x)

        x = self.k_maxpooling(x)
        x = x.view(x.shape[0], -1)
        x = self.fc(x)
        return x
