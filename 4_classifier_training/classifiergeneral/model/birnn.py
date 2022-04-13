# coding:utf-8
import torch
from torch import nn
from model import RNN


class BIRNN(nn.Module):
    def __init__(self, config):
        super(BIRNN, self).__init__()
        self.config = config
        self.direction = 2 if self.config.BIDIRECTIONAL else 1  # 方向数量
        self.embedding = nn.Embedding(self.config.VOCAB_NUM, self.config.EMBEDDING_DIM, padding_idx=0)
        self.rnn = RNN(
            input_size=self.config.EMBEDDING_DIM, hidden_size=self.config.HIDDEN_DIM,
            num_layers=self.config.NUM_LAYER, nonlinearity=self.config.NONLINEARITY,
            bias=self.config.BIAS, dropout=self.config.RNN_DROPOUT, bidirectional=self.config.BIDIRECTIONAL,
            batch_first=True, rnn_type=self.config.RNN_TYPE
        )
        if self.config.IS_ATTENTION:
            # attention所需参数
            self.omega_w = nn.Parameter(torch.zeros(self.config.HIDDEN_DIM*self.direction, self.config.ATTENTION_SIZE))
            self.omega_u = nn.Parameter(torch.zeros(self.config.ATTENTION_SIZE))
            self.linear = nn.Linear(self.config.HIDDEN_DIM * self.direction, self.config.CLASS_NUM)
            self.batch_norm = nn.BatchNorm1d(self.config.HIDDEN_DIM * self.direction)
            # BatchNorm和DropOut使用nn模块下的，而不要使用nn.functional的，因为nn.functional需要手动设置training模式
        else:
            self.linear = nn.Linear(self.config.MAX_LEN * self.config.HIDDEN_DIM * self.direction, self.config.CLASS_NUM)
            self.batch_norm = nn.BatchNorm1d(self.config.MAX_LEN * self.config.HIDDEN_DIM * self.direction)


    def attention_net(self, rnn_output):
        """
        reference: https://github.com/u784799i/biLSTM_attn/blob/master/model.py
        input: rnn_output.size() = (batch_size, squence_length, hidden_size*direction)
        """
        rnn_reshape = rnn_output.reshape(-1, self.config.HIDDEN_DIM*self.direction)  # (squence_length * batch_size, hidden_size*direction)
        attn_tanh = torch.tanh(torch.mm(rnn_reshape, self.omega_w))  # (squence_length * batch_size, attention_size)
        attn_hidden_layer = torch.mm(attn_tanh, self.omega_u.unsqueeze(1))  # (squence_length * batch_size, 1)
        exps = torch.exp(attn_hidden_layer).reshape(-1, self.config.MAX_LEN)  # (batch_size, squence_length)
        alphas = exps / torch.sum(exps, dim=1).unsqueeze(1) # (batch_size, squence_length)
        alphas = alphas.unsqueeze(2)  # (batch_size, squence_length, 1)
        attn_output = torch.sum(rnn_output * alphas, 1)  # (batch_size, hidden_size*direction)
        return attn_output

    def forward(self, x):
        x = self.embedding(x)
        rnn_output, _ = self.rnn(x)  # h_0, c_0 默认为0
        # x = self.maxpool(x)
        if self.config.IS_ATTENTION:
            x = self.attention_net(rnn_output)
        else:
            x = rnn_output.reshape(-1, self.config.MAX_LEN * self.config.HIDDEN_DIM * self.direction)
        x = self.batch_norm(x)
        x = self.linear(x)
        return x
