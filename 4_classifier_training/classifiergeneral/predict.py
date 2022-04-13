# coding:utf-8
import os
import torch
import json
from torch import nn
from config import PredictConfig
from model import TEXTCNN, DPCNN, BIRNN, FASTTEXT, TransformerLC, VDCNN, SVDCNN
from torch.utils.data import DataLoader
from util import ModelType, Logger
from dataloader import TextDataset

config = PredictConfig()
if config.HALF:
    from apex import amp
if config.PRETRAINED:
    config.pretrained_embedding(config.PRETRAINED)


class ClassifierPredict(object):
    def __init__(self):
        self.model = self.model_load()
        self.logger = Logger(config)

    def model_load(self):
        if config.MODEL_METHOD == ModelType.TEXTCNN:
            model = TEXTCNN(config)
        elif config.MODEL_METHOD == ModelType.DPCNN:
            model = DPCNN(config)
        elif config.MODEL_METHOD == ModelType.BIRNN:
            model = BIRNN(config)
        elif config.MODEL_METHOD == ModelType.TransformerLC:
            model = TransformerLC(config)
        elif config.MODEL_METHOD == ModelType.fasttext:
            model = FASTTEXT(config)
        elif config.MODEL_METHOD == ModelType.VDCNN:
            model = VDCNN(config)
        elif config.MODEL_METHOD == ModelType.SVDCNN:
            model = SVDCNN(config)
        else:
            raise ValueError("model must be in {}".format(ModelType.str()))
        model.load_state_dict({k.replace('module.', ''): v for k, v in torch.load(os.path.join(config.ABSOLUTE_PATH, 'config/model.pt'), map_location=torch.device('cpu')).items()})

        model.cuda()
        if config.HALF:
            model = amp.initialize(model, opt_level='O1')
        model.eval()
        return model

    def predict(self, texts, show_text=False, show_prob=True, sep='\t'):
        dataset = TextDataset(texts, config, train=False)
        self.logger.info("the number of data is {}".format(len(dataset)))
        data_loader = DataLoader(dataset=dataset, batch_size=config.BATCH_SIZE, num_workers=config.NUM_WORKERS)

        outputs = []
        probs = []
        texts = []

        for data in data_loader:
            _texts, x = data
            x = x.cuda()
            output = self.model(x)
            output = nn.functional.softmax(output, dim=-1)
            prob, output = torch.max(output, dim=-1)
            if torch.cuda.is_available():
                output = output.cpu()
                prob = prob.cpu()
            probs.extend(prob.tolist())
            outputs.extend(output.int().tolist())
            texts.extend(_texts)
        labels = [config.LABELS[index] for index in outputs]
        if show_prob and show_text:
            outputs = [text + sep + label + sep + '{:.4f}'.format(prob) for text, label, prob in zip(texts, labels, probs)]
        elif show_text:
            outputs = [text + sep + label for text, label in zip(texts, labels)]
        elif show_prob:
            outputs = [label + sep + '{:.4f}'.format(prob) for label, prob in zip(labels, probs)]
        else:
            outputs = labels
        return outputs


if __name__ == '__main__':
    texts = []
    fr = open('贷款审核错误.txt')
    for line in fr:
        texts.append(line.strip())

    predictor = ClassifierPredict()
    result = predictor.predict(texts)
    for i in range(len(result)):
        print(str(result[i]) + ' ' + texts[i] )
    # print('\n'.join(list(zip(result, texts))))
