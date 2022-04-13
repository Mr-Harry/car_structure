# coding:utf-8
import torch
from torch import nn

# reference : 
# https://arxiv.org/pdf/1901.09821.pdf
# https://blog.csdn.net/weixin_30793735/article/details/88915612?depth_1-
# The training is also performed
# with SGD, utilizing size batch of 64, with a maximum of 100
# epochs. We use an initial learning rate of 0.01, a momentum
# of 0.9 and a weight decay of 0.001
class DepthWiseBlock(nn.Module):
    def __init__(self, in_channels, use_bias=True, stride=1):
        super(DepthWiseBlock, self).__init__()
        self.depthwise_block = nn.Sequential(
            nn.Conv1d(in_channels, in_channels, kernel_size=3, stride=stride, padding=1, bias=use_bias, groups=in_channels),
            nn.BatchNorm1d(in_channels),
            nn.ReLU()
        )
    
    def forward(self, x):
        return self.depthwise_block(x)


class PointWiseBlock(nn.Module):
    def __init__(self, in_channels, out_channels, use_bias=True, stride=1, use_relu=True):
        super(PointWiseBlock, self).__init__()
        self.use_relu = use_relu
        self.pointwise_block = nn.Sequential(
            nn.Conv1d(in_channels, out_channels, kernel_size=1, bias=use_bias, stride=stride),
            nn.BatchNorm1d(out_channels)
        )
        self.relu = nn.ReLU()
    
    def forward(self, x):
        x = self.pointwise_block(x)
        if self.use_relu:
            return self.relu(x)
        else:
            return x


class IdentityTDSCBlock(nn.Module):
    def __init__(self, channels, use_bias=True, short_cut=True):
        super(IdentityTDSCBlock, self).__init__()
        self.short_cut = short_cut
        self.block = nn.Sequential(
            DepthWiseBlock(channels, use_bias=use_bias),
            PointWiseBlock(channels, channels, use_bias=use_bias, use_relu=True),
            DepthWiseBlock(channels, use_bias=use_bias),
            PointWiseBlock(channels, channels, use_bias=use_bias, use_relu=False)
        )
        self.relu = nn.ReLU()
    
    def forward(self, x):
        out = self.block(x)
        if self.short_cut:
            out = torch.add(x, out)
        out = self.relu(out)
        return out


class ResidualTDSCBlock(nn.Module):
    def __init__(self, channels, use_bias=True, short_cut=False, pool_type='max'):
        super(ResidualTDSCBlock, self).__init__()
        self.short_cut = short_cut
        self.pool_type = pool_type

        self.block = nn.Sequential(
            DepthWiseBlock(channels, use_bias=use_bias),
            PointWiseBlock(channels, channels, use_bias=use_bias, use_relu=True),
            DepthWiseBlock(channels, use_bias=use_bias),
            PointWiseBlock(channels, channels, use_bias=use_bias, use_relu=False)
        )
        self.residual_conv = nn.Sequential(
            DepthWiseBlock(channels, stride=2, use_bias=use_bias),
            PointWiseBlock(channels, channels, use_bias=use_bias, use_relu=False)
        )
        self.down_sample_block = DownSampleBlock(pool_type, channels)
        self.relu = nn.ReLU()
        if pool_type:
            self.up_conv = nn.Sequential(
                DepthWiseBlock(channels, use_bias=use_bias),
                PointWiseBlock(channels, channels * 2, use_bias=use_bias, use_relu=False),
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


class DownSampleBlock(nn.Module):
    def __init__(self, pool_type='max', channels=None, use_bias=True):
        super(DownSampleBlock, self).__init__()
        self.pool_type = pool_type
        if self.pool_type == 'max':
            self.pool = nn.MaxPool1d(kernel_size=3, stride=2, padding=1)
        elif self.pool_type == 'conv':
            self.block = nn.Sequential(
                DepthWiseBlock(channels, stride=2, use_bias=use_bias),
                PointWiseBlock(channels, channels, use_bias=use_bias, use_relu=False)
            )
    def forward(self, x):
        if self.pool_type == 'max':
            return self.pool(x)
        elif self.pool_type == 'conv':
            x = self.block(x)
            return x
        elif self.pool_type is None:
            return x
        else:
            raise ValueError("unsupported pooling type , must be ['max', 'conv', None] !")


class KMaxPooling(nn.Module):
    def __init__(self, k):
        super(KMaxPooling, self).__init__()
        self.k = k
    
    def forward(self, x):
        x = torch.topk(x, self.k, dim=2)[0]
        return x


class SVDCNN(nn.Module):
    def __init__(self, config=None):
        super(SVDCNN, self).__init__()
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

        self.embedding = nn.Embedding(self.config.VOCAB_NUM, 16, padding_idx=0)
        self.conv = nn.Sequential(
            DepthWiseBlock(16, use_bias=True),
            PointWiseBlock(16, 64, use_bias=True, use_relu=True)
        )
        
        self.identity_block64  = IdentityTDSCBlock(64, use_bias=True, short_cut=True)
        self.identity_block128 = IdentityTDSCBlock(128, use_bias=True, short_cut=True)
        self.identity_block256 = IdentityTDSCBlock(256, use_bias=True, short_cut=True)
        self.identity_block512 = IdentityTDSCBlock(512, use_bias=True, short_cut=True)

        self.conv_block64 = ResidualTDSCBlock(64, use_bias=True, short_cut=True, pool_type='max')
        self.conv_block128 = ResidualTDSCBlock(128, use_bias=True, short_cut=True, pool_type='max')
        self.conv_block256 = ResidualTDSCBlock(256, use_bias=True, short_cut=True, pool_type='max')
        self.conv_block512 = ResidualTDSCBlock(512, use_bias=True, short_cut=False, pool_type=None)

        # self.k_maxpooling = KMaxPooling(8)
        self.avg_pooling = nn.AvgPool1d(kernel_size=11, stride=1)

        self.fc = nn.Linear(4096, self.config.CLASS_NUM)

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

        # x = self.k_maxpooling(x)
        x = self.avg_pooling(x)
        x = x.view(x.shape[0], -1)
        x = self.fc(x)
        return x
