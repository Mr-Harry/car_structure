# coding:utf-8
import os
import torch
import json
import shutil
import warnings
from torch import nn
from torch.utils.data import DataLoader
from torch.utils.data.distributed import DistributedSampler
from sklearn.metrics import recall_score, accuracy_score, f1_score, precision_score
from model import TEXTCNN, DPCNN, BIRNN, TransformerLC, FASTTEXT, VDCNN, SVDCNN
from loss import LabelSmoothingLoss
from dataloader import DatasetLoader
from config import TrainConfig
from util import ModelType, Logger, train_parser
from apex import amp


class Classifier(object):
    def __init__(self, args, config):
        self.args = args
        self.config = config

        if not self.config.TRAIN_RATE:  # 是否训练
            raise ValueError("TRAIN_RATE must be not zero !")

        # 创建保存结果文件夹
        if not os.path.exists(self.config.CHECKPOINT_FOLD): os.mkdir(self.config.CHECKPOINT_FOLD)
        self.config.SAVE_FOLD = os.path.join(self.config.CHECKPOINT_FOLD, self.args.model_name)
        if not os.path.exists(self.config.SAVE_FOLD): os.mkdir(self.config.SAVE_FOLD)

        self.logger = Logger(self.config)
        self.loader = DatasetLoader(self.config)
        if self.args.pretrained is not None:
            self.config.pretrained_embedding(self.args.pretrained)

        # 损失函数
        if self.config.WEIGHTED_CLASS:
            counts = [self.loader.label_counts[label] for _, label in enumerate(self.config.LABELS)]
            # weight = torch.FloatTensor(list(map(lambda x: max(counts) / float(x), counts))).cuda()
            weight = torch.FloatTensor(list(map(lambda x: 1 / float(x), counts))).cuda()
            self.loss_func = LabelSmoothingLoss(weight=weight)
        else:
            # self.loss_func = torch.nn.CrossEntropyLoss()
            self.loss_func = LabelSmoothingLoss()

        # 加载模型
        self.model = self.build()
        self.initialize()

    def build(self):
        if self.config.MODEL_METHOD == ModelType.TEXTCNN:
            model = TEXTCNN(self.config)
        elif self.config.MODEL_METHOD == ModelType.DPCNN:
            model = DPCNN(self.config)
        elif self.config.MODEL_METHOD == ModelType.BIRNN:
            model = BIRNN(self.config)
        elif self.config.MODEL_METHOD == ModelType.TransformerLC:
            model = TransformerLC(self.config)
        elif self.config.MODEL_METHOD == ModelType.fasttext:
            model = FASTTEXT(self.config)
        elif self.config.MODEL_METHOD == ModelType.VDCNN:
            model = VDCNN(self.config)
        elif self.config.MODEL_METHOD == ModelType.SVDCNN:
            model = SVDCNN(self.config)
        else:
            raise ValueError("model must be in {}".format(ModelType.str()))
        return model

    def initialize(self):
        # 优化器
        if self.config.OPTIMIZER == 'Adam':
            self.optimizer = torch.optim.Adam(self.model.parameters(), lr=self.config.LEARNING_RATE,
                                              weight_decay=0.0001)
        elif self.config.OPTIMIZER == 'SGD':
            self.optimizer = torch.optim.SGD(self.model.parameters(), lr=self.config.LEARNING_RATE,
                                             momentum=self.config.MOMENTUM)
        elif self.config.OPTIMIZER == 'RMSprop':
            self.optimizer = torch.optim.RMSprop(self.model.parameters(), lr=self.config.LEARNING_RATE)
        else:
            raise ValueError("optimizer must be in {}".format(['Adam', 'SGD', 'RMSprop']))

        # GPU设置
        if self.args.gpu_num:
            self.model.cuda()
            if self.config.HALF:
                model, self.optimizer = amp.initialize(self.model, self.optimizer, opt_level='O1')
            if self.args.gpu_num > 1:  # 多卡训练
                self.model = torch.nn.parallel.DistributedDataParallel(self.model, device_ids=[self.args.local_rank],
                                                                       output_device=self.args.local_rank,
                                                                       find_unused_parameters=True)
        else:
            self.logger.info("now is using CPU")

    def accuracy_score_torch(self, y_true, y_pred):
        return torch.sum(y_true == y_pred).item()/y_true.shape[0]

    def train(self, dataset):
        self.model.train()
        if self.args.gpu_num > 1:
            dataloader = DataLoader(dataset, batch_size=self.config.BATCH_SIZE, num_workers=self.config.NUM_WORKERS, sampler=DistributedSampler(dataset))
        else:
            dataloader = DataLoader(dataset, batch_size=self.config.BATCH_SIZE, num_workers=self.config.NUM_WORKERS)
        acc_sum = 0
        loss_sum = 0

        for _, x_batch, y_batch in dataloader:
            if self.args.gpu_num:
                x_batch = x_batch.cuda()
                y_batch = y_batch.cuda()
            output = self.model(x_batch)
            loss = self.loss_func(output, y_batch)
            loss = (loss - 0.45).abs() + 0.45 # with flood
            self.optimizer.zero_grad()
            if self.config.HALF:
                with amp.scale_loss(loss, self.optimizer) as scale_loss:
                    scale_loss.backward()
            else:
                loss.backward()
            self.optimizer.step()

        if self.args.local_rank == 0:
            output = torch.argmax(output, dim=-1)
            acc = self.accuracy_score_torch(output, y_batch)
            self.logger.info("train : final loss is {}, final acc is {:.4f}".format(loss, acc))

    def eval(self, dataset, top_k=1, save_text=False):
        self.model.eval()
        dataloader = DataLoader(dataset, batch_size=self.config.BATCH_SIZE, num_workers=self.config.NUM_WORKERS)
        texts = []
        y_true = []
        y_pred = []
        probs = []
        for text_batch, x_batch, y_batch in dataloader:
            if self.args.gpu_num:
                x_batch = x_batch.cuda()
                y_batch = y_batch.cuda()
            output = self.model(x_batch)
            loss = self.loss_func(output, y_batch)
            output = nn.functional.softmax(output, dim=-1)
            prob, index = output.topk(top_k, dim=-1)
            if self.args.gpu_num:
                y_batch = y_batch.cpu()
                index = index.cpu()
                prob = prob.cpu()
            y_true += y_batch.int().tolist()
            y_pred += index.int().tolist()
            probs += prob.squeeze(dim=-1).tolist()
            texts += text_batch
        accuracy = Classifier.accuracy_topk(y_true, y_pred)
        if top_k == 1:
            if self.args.local_rank == 0:
                self.logger.info("validate : final loss is {}, final acc is {:.4f}".format(loss, accuracy))
            # 输出每个类别的验证指标
            precision = precision_score(y_true, y_pred, average=None)
            recall = recall_score(y_true, y_pred, average=None)
            f1 = f1_score(y_true, y_pred, average=None)
            for _label, _precision, _recall, _f1 in zip(self.config.LABELS, precision, recall, f1):
                if self.args.local_rank == 0: self.logger.info('{} {:.4f} {:.4f} {:.4f}'.format(_label, _precision, _recall, _f1))
            
            # 输出整体验证指标，"average" 可选择 'weighted', 'micro', 'macro'
            precision = precision_score(y_true, y_pred, average='macro')
            recall = recall_score(y_true, y_pred, average='macro')
            f1 = f1_score(y_true, y_pred, average='macro')
            if self.args.local_rank == 0:
                self.logger.info('macro {:.4f} {:.4f} {:.4f}'.format(precision, recall, f1))

            precision = precision_score(y_true, y_pred, average='micro')
            recall = recall_score(y_true, y_pred, average='micro')
            f1 = f1_score(y_true, y_pred, average='micro')
            if self.args.local_rank == 0:
                self.logger.info('micro {:.4f} {:.4f} {:.4f}'.format(precision, recall, f1))

            precision = precision_score(y_true, y_pred, average='weighted')
            recall = recall_score(y_true, y_pred, average='weighted')
            f1 = f1_score(y_true, y_pred, average='weighted')
            if self.args.local_rank == 0:
                self.logger.info('weighted {:.4f} {:.4f} {:.4f}'.format(precision, recall, f1))
        else:
            if self.args.local_rank == 0: self.logger.info('top_{} accuracy={:.4f}'.format(top_k, accuracy))
        
        if save_text:
            y_pred = [elem[0] for elem in y_pred]
            res = [self.config.SEP.join([text, self.config.LABELS[label_t], self.config.LABELS[label_p], str(prob)]) for text, label_t, label_p, prob in zip(texts, y_true, y_pred, probs)]
            with open(os.path.join(self.config.SAVE_FOLD, 'validate_predict.txt'), 'w+', encoding='utf-8') as f:
                print('\n'.join(res), file=f)

    @staticmethod
    def accuracy_topk(y_true, y_pred):
        cnt = 0
        for _true, _pred in zip(y_true, y_pred):
            for elem in _pred:
                if _true == elem:
                    cnt += 1
                    break
        return cnt/len(y_pred)

    def save_model(self):
        if self.args.local_rank == 0:
            self.logger.info("start save model")
            torch.save(self.model.state_dict(), os.path.join(self.config.SAVE_FOLD, 'model.pt'))
            shutil.copy(os.path.join(self.config.ABSOLUTE_PATH, self.args.label_path), self.config.SAVE_FOLD)

            if self.config.MODEL_METHOD == 'fasttext':
                self.logger.info("start save embedding weight")
                vocab_emb = {}
                if self.args.gpu_num:
                    self.model = self.model.to(torch.device("cpu"))
                for vocab, num in self.config.VOCABS.items():
                    vocab_emb[vocab] = self.model.module.embedding(torch.tensor(num)).tolist()
                with open(os.path.join(self.config.SAVE_FOLD, 'vocab_emb.json'), 'w', encoding='utf-8') as f:
                    json.dump(vocab_emb, f, ensure_ascii=False)
            
            with open(os.path.join(self.config.SAVE_FOLD, 'config.json'), 'w', encoding='utf-8') as f:
                json.dump(self.args.__dict__, f)

    def main(self):
        if self.config.TRAIN_RATE:
            if self.args.local_rank == 0: self.logger.info("start training")
            for epoch in range(self.config.NUM_EPOCHS):
                if self.args.local_rank == 0: self.logger.info("epoch {} / {} :".format(epoch+1, self.config.NUM_EPOCHS))
                self.train(self.loader.train)
                if self.config.VALIDATE_RATE:
                    self.eval(self.loader.validate)
        if self.config.VALIDATE_RATE:  # 验证
            if self.args.local_rank == 0: self.logger.info("start validate")
            self.eval(self.loader.validate, top_k=1, save_text=True)
            self.eval(self.loader.validate, top_k=2)
            self.eval(self.loader.validate, top_k=3)
        if self.config.TEST_RATE:  # 测试
            if self.args.local_rank == 0: self.logger.info("start test")
            self.eval(self.loader.test, top_k=1, save_text=True)
            self.eval(self.loader.test, top_k=2)
            self.eval(self.loader.test, top_k=3)
        if self.config.TRAIN_RATE:
            self.save_model()


if __name__ == '__main__':
    warnings.filterwarnings("ignore")
    args = train_parser()
    args.gpu_num = torch.cuda.device_count()

    if args.gpu_num > 1:
        torch.distributed.init_process_group(backend="nccl")
        args.local_rank = torch.distributed.get_rank()
        torch.cuda.set_device(args.local_rank)
        device = torch.device("cuda", args.local_rank)

    config = TrainConfig(args)
    clf = Classifier(args, config)
    clf.main()

