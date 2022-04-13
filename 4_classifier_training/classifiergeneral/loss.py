import torch
from torch import nn
import torch.nn.functional as F

class MultiLabelLoss(nn.Module):
    """
    多标签损失函数
    """
    def __init__(self, weight=None, size_average=None, reduce=None, reduction='mean'):
        super(MultiLabelLoss, self).__init__()
        self.sigmoid = nn.Sigmoid()
        self.bce = nn.BCELoss(weight=None, size_average=None, reduce=None, reduction='mean')

    def forward(self, inputs, target):
        inputs = nn.Softmax(inputs)
        output = self.bce(inputs, target)
        return output



class CircleLoss(nn.Module):
    def __init__(self, scale=32, margin=0.25, similarity='cos', **kwargs):
        super(CircleLoss, self).__init__()
        self.scale = scale
        self.margin = margin
        self.similarity = similarity

    def forward(self, feats, labels):
        assert feats.size(0) == labels.size(0), \
            f"feats.size(0): {feats.size(0)} is not equal to labels.size(0): {labels.size(0)}"

        m = labels.size(0)
        mask = labels.expand(m, m).t().eq(labels.expand(m, m)).float()
        pos_mask = mask.triu(diagonal=1)
        neg_mask = (mask - 1).abs_().triu(diagonal=1)
        if self.similarity == 'dot':
            sim_mat = torch.matmul(feats, torch.t(feats))
        elif self.similarity == 'cos':
            feats = F.normalize(feats)
            sim_mat = feats.mm(feats.t())
        else:
            raise ValueError('This similarity is not implemented.')

        pos_pair_ = sim_mat[pos_mask == 1]
        neg_pair_ = sim_mat[neg_mask == 1]

        alpha_p = torch.relu(-pos_pair_ + 1 + self.margin)
        alpha_n = torch.relu(neg_pair_ + self.margin)
        margin_p = 1 - self.margin
        margin_n = self.margin
        loss_p = torch.sum(torch.exp(-self.scale * alpha_p * (pos_pair_ - margin_p)))
        loss_n = torch.sum(torch.exp(self.scale * alpha_n * (neg_pair_ - margin_n)))
        loss = torch.log(1 + loss_p * loss_n)
        return loss


class LabelSmoothingLoss(torch.nn.Module):
    def __init__(self, smoothing=0.06, 
                 reduction="mean", weight=None):
        super(LabelSmoothingLoss, self).__init__()
        self.smoothing = smoothing
        self.reduction = reduction
        self.weight = weight

    def reduce_loss(self, loss):
        return loss.mean() if self.reduction == 'mean' else loss.sum() \
         if self.reduction == 'sum' else loss

    def linear_combination(self, x, y):
        return self.smoothing * x + (1 - self.smoothing) * y

    def forward(self, preds, target):
        assert 0 <= self.smoothing < 1

        if self.weight is not None:
            self.weight = self.weight.to(preds.device)

        n = preds.size(-1)
        log_preds = F.log_softmax(preds, dim=-1)
        loss = self.reduce_loss(-log_preds.sum(dim=-1))
        nll = F.nll_loss(
            log_preds, target, reduction=self.reduction, weight=self.weight
        )
        return self.linear_combination(loss / n, nll)
