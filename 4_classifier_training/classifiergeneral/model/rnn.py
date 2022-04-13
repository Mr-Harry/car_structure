# coding:utf-8
import torch
from torch import nn
from util import Type


class RNNType(Type):
    RNN = 'RNN'
    LSTM = 'LSTM'
    GRU = 'GRU'

    @classmethod
    def str(cls):
        return ",".join([cls.RNN, cls.LSTM, cls.GRU])


class RNN(nn.Module):
    """
    One layer rnn.
    """

    def __init__(self, input_size, hidden_size, num_layers=1,
                 nonlinearity="tanh", bias=True, batch_first=True, dropout=0,
                 bidirectional=False, rnn_type=None, **kwargs):
        super(RNN, self).__init__()
        self.rnn_type = rnn_type
        self.num_layers = num_layers
        self.batch_first = batch_first
        self.bidirectional = bidirectional
        if rnn_type == RNNType.LSTM:
            self.rnn = nn.LSTM(
                input_size, hidden_size,
                num_layers=num_layers, bias=bias,
                batch_first=batch_first, dropout=dropout,
                bidirectional=bidirectional, **kwargs)
        elif rnn_type == RNNType.GRU:
            self.rnn = nn.GRU(
                input_size, hidden_size,
                num_layers=num_layers, bias=bias,
                batch_first=batch_first, dropout=dropout,
                bidirectional=bidirectional, **kwargs)
        elif rnn_type == RNNType.RNN:
            self.rnn = nn.RNN(
                input_size, hidden_size, nonlinearity=nonlinearity, bias=bias,
                batch_first=batch_first, dropout=dropout,
                bidirectional=bidirectional, **kwargs)
        else:
            raise TypeError(
                "Unsupported rnn init type: %s. Supported rnn type is: %s" % (
                    rnn_type, RNNType.str()))

    def forward(self, x):
        output = self.rnn(x)
        return output
