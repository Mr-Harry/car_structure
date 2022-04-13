# coding:utf-8
import torch
import torch.nn as nn
import torch.functional as F
import numpy as np


def padding_mask(query, key):
    """For masking out the padding part of the keys sequence.
    Args:
    seq_k: Keys tensor, with shape [B, L_k]
    seq_q: Query tensor, with shape [B, L_q]
    Returns:
    A masking tensor, with shape [B, L_1, L_k]
    """
    len_k = key.size(1)
    pad_mask = query.eq(0)  # 输出是否等于0的bool型矩阵
    pad_mask = pad_mask.unsqueeze(1).expand(-1, len_k, -1)  # shape [B, L_k, L_q]
    return pad_mask


def sequence_mask(x):
    batch_size, len_x = x.size()
    mask = torch.triu(torch.ones(len_x, len_x), diagonal=1)
    mask = mask.unsqueeze(0).expand(batch_size, -1, -1)
    return mask


class ScaledDotProductAttention(nn.Module):
    def __init__(self, attention_dropout=0.0):
        super(ScaledDotProductAttention, self).__init__()
        self.dropout = nn.Dropout(attention_dropout)
        self.softmax = nn.Softmax(dim=2)

    def forward(self, q, k, v, scale=None, mask=None):
        """
        前向传播.
        Args:
        	q: Queries张量，形状为[B, L_q, D_q]
        	k: Keys张量，形状为[B, L_k, D_k]
        	v: Values张量，形状为[B, L_v, D_v]，一般来说就是q==k==v
        	scale: 缩放因子，一个浮点标量
        	attn_mask: Masking张量，形状为[B, L_q, L_k]

        Returns:
        	上下文张量和attention张量
        """
        attention = torch.bmm(q, k.transpose(1, 2))
        if scale:
            attention = attention * scale
        if mask is not None:
            # 给需要 mask 的地方设置一个负无穷
            attention = attention.masked_fill_(mask, -np.inf)
	    # 计算softmax
        attention = self.softmax(attention)
	    # 添加dropout
        attention = self.dropout(attention)
	    # 和V做点积
        outputs = torch.bmm(attention, v)
        return outputs, attention


class MultiHeadAttention(nn.Module):
    def __init__(self, model_dim=512, n_head=8, dropout=0.0):
        super(MultiHeadAttention, self).__init__()

        self.dim_per_head = model_dim // n_head
        assert self.dim_per_head * n_head == model_dim, "n_head must be divide by model_dim"
        self.n_head = n_head

        self.linear_q = nn.Linear(model_dim, model_dim)
        self.linear_k = nn.Linear(model_dim, model_dim)
        self.linear_v = nn.Linear(model_dim, model_dim)
        
        self.fc = nn.Linear(model_dim, model_dim, bias=False)
        self.dot_product_attention = ScaledDotProductAttention(dropout)
        self.dropout = nn.Dropout(dropout)
        self.layer_norm = nn.LayerNorm(model_dim, eps=1e-6)

    def forward(self, q, k, v, mask=None):
        residual = q  # 残差连接
        batch_size = q.size(0)

        q = self.linear_q(q)
        k = self.linear_k(k)
        v = self.linear_v(v)

        scale = k.size(-1) ** -0.5
        output, attn = self.dot_product_attention(q, k, v, scale=scale, mask=mask)

        output = self.dropout(self.fc(output))
        output = self.layer_norm(output + residual)

        return output, attn


class PositionwiseFeedForward(nn.Module):
    ''' A two-feed-forward-layer module '''

    def __init__(self, d_in, d_hid, dropout=0.0):
        super().__init__()
        self.w_1 = nn.Linear(d_in, d_hid) # position-wise
        self.w_2 = nn.Linear(d_hid, d_in) # position-wise
        self.layer_norm = nn.LayerNorm(d_in, eps=1e-6)
        self.dropout = nn.Dropout(dropout)
        self.relu = nn.ReLU()

    def forward(self, x):
        residual = x

        output = self.w_2(self.relu(self.w_1(x)))
        output = self.dropout(output)
        output = self.layer_norm(output + residual)

        return output


class PositionalEmbedding(nn.Module):
    def __init__(self, d_model, max_len, dropout):
        super(PositionalEmbedding, self).__init__()
        self.dropout = nn.Dropout(p=dropout)

        pe = torch.tensor([
            [pos/np.power(10000, 2.0*(j//2)/d_model) for j in range(d_model)]
            for pos in range(max_len)
        ])
        pe[:, 0::2] = torch.sin(pe[:, 0::2])
        pe[:, 1::2] = torch.cos(pe[:, 1::2])
        self.register_buffer('pe', pe)  # [5000, d_model]

    def forward(self, x):
        x = x + self.pe.unsqueeze(0).repeat(x.size(0), 1, 1)
        return self.dropout(x)


class EncoderLayer(nn.Module):
    def __init__(self, model_dim=512, n_head=8, ffn_dim=2048, dropout=0.0):
        super(EncoderLayer, self).__init__()
        self.attention = MultiHeadAttention(model_dim, n_head, dropout)
        self.feed_forward = PositionwiseFeedForward(model_dim, ffn_dim, dropout)

    def forward(self, x, mask=None):
        x, attn = self.attention(x, x, x, mask)
        output = self.feed_forward(x)
        return output, attn


class Encoder(nn.Module):
    def __init__(self, model_dim, n_head, dropout, max_len, vocab_num, num_layers, ffn_dim):
        super(Encoder, self).__init__()
        self.encoded_layers = nn.ModuleList([
            EncoderLayer(model_dim, n_head, ffn_dim, dropout) for _ in range(num_layers)
        ])
        self.emb = nn.Embedding(vocab_num, model_dim, padding_idx=0)
        self.position_emb = PositionalEmbedding(model_dim, max_len, dropout)

    def forward(self, x):
        mask = padding_mask(x, x)
        x = self.emb(x)
        output = self.position_emb(x)

        attns = []
        for layer in self.encoded_layers:
            output, attn = layer(output, mask)
            attns.append(attn)
        return output, attns


class DecoderLayer(nn.Module):
    def __init__(self, model_dim=512, n_head=8, ffn_dim=2048, dropout=0.0):
        super(DecoderLayer, self).__init__()
        self.attention = MultiHeadAttention(model_dim, n_head, dropout)
        self.feed_forward = PositionwiseFeedForward(model_dim, ffn_dim, dropout)
        self.layer_norm = nn.LayerNorm(model_dim, eps=1e-6)

    def forward(self, decoder_input, encoder_output, self_attn_mask=None, encoder_decoder_attn_mask=None):
        # self-attention
        decoder_output, self_attn = self.attention(decoder_input, decoder_input, decoder_input, self_attn_mask)
        # encoder-decoder attention
        decoder_output, encoder_decoder_attn = self.attention(decoder_output, encoder_output, encoder_output, encoder_decoder_attn_mask)

        decoder_output = self.feed_forward(decoder_output)
        return decoder_output, self_attn, encoder_decoder_attn


class Decoder(nn.Module):
    def __init__(self, model_dim, n_head, dropout, max_len, vocab_num, num_layers, ffn_dim):
        super(Decoder, self).__init__()
        self.emb = nn.Embedding(vocab_num, model_dim, padding_idx=0)
        self.position_emb = PositionalEmbedding(model_dim, max_len, dropout)
        self.decoder_layers = nn.ModuleList([
            DecoderLayer(model_dim, n_head, ffn_dim, dropout) for _ in range(num_layers)
        ])

    def forward(self, dec_input, enc_output, context_attn_mask=None):
        x = self.emb(dec_input)
        output = self.position_emb(x)

        pad_mask = padding_mask(dec_input, dec_input)
        seq_mask = sequence_mask(dec_input)
        self_attn_mask = torch.gt((pad_mask + seq_mask), 0)

        self_attns = []
        encoder_decoder_attns = []
        for layer in self.decoder_layers:
            output, self_attn, encoder_decoder_attn = layer(output, enc_output, self_attn_mask, context_attn_mask)
            self_attns.append(self_attn)
            encoder_decoder_attns.append(encoder_decoder_attn)

        return output, self_attns, encoder_decoder_attns


class TransformerModule(nn.Module):
    def __init__(self, src_vocab_size, src_max_len, tgt_vocab_size, tgt_max_len, num_layers=6, model_dim=512, num_heads=8, ffn_dim=2048, dropout=0.2):
        super(TransformerModule, self).__init__()
        self.encoder = Encoder(model_dim, num_heads, dropout, src_max_len, src_vocab_size, num_layers, ffn_dim)
        self.decoder = Decoder(model_dim, num_heads, dropout, tgt_max_len, tgt_vocab_size, num_layers, ffn_dim)
        self.fc = nn.Linear(model_dim, tgt_vocab_size, bias=False)
        self.softmax = nn.Softmax(dim=-1)

    def forward(self, src, tgt):
        context_attn_mask = padding_mask(src, tgt)
        output, enc_self_attn = self.encoder(src)
        output, dec_self_attn, encoder_decoder_attn = self.decoder(tgt, output, context_attn_mask)
        output = self.fc(output)
        output = self.softmax(output)
        return output, enc_self_attn, dec_self_attn, encoder_decoder_attn


if __name__ == '__main__':
    # encoder = Encoder(512, 8, 0.2, 120, 1000, 6, 2048)
    # decoder = Decoder(512, 8, 0.2, 140, 800, 6, 2048)
    enc_x = torch.rand(32, 120)
    dec_x = torch.rand(32, 140)
    enc_x = torch.randint_like(enc_x, 0, 1000).long()
    dec_x = torch.randint_like(dec_x, 0, 800).long()

    # output, attns = encoder(enc_x)
    # output, self_attns, encoder_decoder_attns = decoder(dec_x, output)
    tran = TransformerModule(1000, 120, 800, 140)
    output, enc_self_attn, dec_self_attn, encoder_decoder_attn = tran(enc_x, dec_x)
    print(output.shape)
