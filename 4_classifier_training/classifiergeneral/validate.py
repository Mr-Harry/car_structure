# coding:utf-8
import os
import torch
import json
from torch import nn
from config import PredictConfig
from model import TEXTCNN, DPCNN, BIRNN, FASTTEXT, TransformerLC, VDCNN, SVDCNN
from sklearn.metrics import recall_score, accuracy_score, f1_score, precision_score, classification_report
from torch.utils.data import DataLoader
from util import ModelType, Logger, validate_parser
from dataloader import TextDataset

args = validate_parser()
config = PredictConfig()
if args.pretrained:
    config.PRETRAINED = config.pretrained_embedding(args.pretrained)
if config.HALF:
    from apex import amp


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
    
    def run(self):
        texts = []
        y_true = []
        with open(args.read_path, 'r', encoding='utf-8') as fr:
            for line in fr:
                line = line.strip().split(args.read_sep)
                texts.append(line[0])
                y_true.append(line[1])

        y_pred = self.predict(texts, show_prob=False)
        # precision = precision_score(y_true, y_pred, average=None)
        # recall = recall_score(y_true, y_pred, average=None)
        # f1 = f1_score(y_true, y_pred, average=None)
        
        #for _label, _precision, _recall, _f1 in zip(set(y_), precision, recall, f1):
        #    self.logger.info(
        #        '{} {:.4f} {:.4f} {:.4f} {}'.format(_label, _precision, _recall, _f1, len(_label)))
        self.logger.info(classification_report(y_true, y_pred))

        # 输出整体验证指标，"average" 可选择 'weighted', 'micro', 'macro'
        precision = precision_score(y_true, y_pred, average='macro')
        recall = recall_score(y_true, y_pred, average='macro')
        f1 = f1_score(y_true, y_pred, average='macro')
        self.logger.info('macro {:.4f} {:.4f} {:.4f}'.format(precision, recall, f1))

        precision = precision_score(y_true, y_pred, average='micro')
        recall = recall_score(y_true, y_pred, average='micro')
        f1 = f1_score(y_true, y_pred, average='micro')
        self.logger.info('micro {:.4f} {:.4f} {:.4f}'.format(precision, recall, f1))

        precision = precision_score(y_true, y_pred, average='weighted')
        recall = recall_score(y_true, y_pred, average='weighted')
        f1 = f1_score(y_true, y_pred, average='weighted')
        self.logger.info('weighted {:.4f} {:.4f} {:.4f}'.format(precision, recall, f1))
        

if __name__ == '__main__':
    predictor = ClassifierPredict()
    predictor.run()


