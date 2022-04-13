# coding:utf-8
import os
import torch
import json
from torch import nn
from config import MultiPredictConfig
from model import TEXTCNN, DPCNN, BIRNN, FASTTEXT, TransformerLC, VDCNN, SVDCNN
from util import ModelType, Logger, predict_parser
from torch.utils.data import DataLoader
from torch.utils.data.distributed import DistributedSampler
from dataloader import TextDataset

args = predict_parser()
args.gpu_num = torch.cuda.device_count()
config = MultiPredictConfig(args)
if args.half:
    from apex import amp
if config.PRETRAINED:
    config.pretrained_embedding(config.PRETRAINED)

if args.gpu_num > 1:
    torch.distributed.init_process_group(backend="nccl")
    args.local_rank = torch.distributed.get_rank()
    torch.cuda.set_device(args.local_rank)
    device = torch.device("cuda", args.local_rank)


class Predicter(object):
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
        model.load_state_dict({k.replace('module.', ''): v for k, v in torch.load(os.path.join(config.MODEL_FOLD, 'model.pt'), map_location=torch.device('cpu')).items()})

        model.cuda()
        if config.HALF:
            model = amp.initialize(model, opt_level='O1')
        if torch.cuda.device_count() > 1:
            model = torch.nn.parallel.DistributedDataParallel(model, device_ids=[args.local_rank], output_device=args.local_rank)
        model.eval()
        return model

    def predict(self, texts):
        dataset = TextDataset(texts, config, train=False)
        self.logger.info("rank {} start,  the number of data is {}".format(args.local_rank, len(dataset)//2))
        if args.gpu_num > 1:
            data_loader = DataLoader(dataset=dataset,
                                    batch_size=args.batch_size,
                                    num_workers=args.num_workers,
                                    sampler=DistributedSampler(dataset))
        else:
            data_loader = DataLoader(dataset=dataset,
                        batch_size=args.batch_size,
                        num_workers=args.num_workers)

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
        outs = self.format_output(texts, labels, probs)
        self.logger.info("rank {} end".format(args.local_rank))
        return outs
                
    def format_output(self, texts, labels, probs):
        if args.read_sep == args.write_sep:
            return [text + args.write_sep + label + args.write_sep + '{:.4f}'.format(prob) for text, label, prob in zip(texts, labels, probs)]
        else:
            return [args.write_sep.join(text.split(args.read_sep)) + args.write_sep + label + args.write_sep + '{:.4f}'.format(prob)
                            for text, label, prob in zip(texts, labels, probs)]


def read_file(file_path, chunk_size=10240000):
    with open(file_path, 'r', encoding='utf-8') as f:
        output = []
        for i, line in enumerate(f):
            output.append(line.rstrip('\n'))
            if not (i + 1) % chunk_size:
                yield output
                output = []
    if len(output):
        yield output


def main():
    predictor = Predicter()
    for texts in read_file(args.read_path):
        # print("texts:",texts)
        result = predictor.predict(texts)
        # print("result:",result)
        with open(args.write_path, 'a+', encoding='utf-8') as f:
            print('\n'.join(result), file=f)


if __name__ == '__main__':
    main()
