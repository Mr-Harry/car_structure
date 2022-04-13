# coding:utf-8
import os

import numpy as np
import torch
import warnings
from torch import nn
from torch.utils.data import DataLoader
from cleanlab.pruning import get_noise_indices
from train import Classifier
from config import CrossValTrainConfig
from util import train_parser


class MultiClassifier(Classifier):
    def __init__(self, args, config):
        self.args = args
        self.config = config
        super(MultiClassifier, self).__init__(args, config)

    def eval(self, dataset, top_k=None, save_text=None):
        self.model.eval()
        dataloader = DataLoader(dataset, batch_size=self.config.BATCH_SIZE, num_workers=self.config.NUM_WORKERS)
        outputs = []
        for _, x_batch, y_batch in dataloader:
            if self.args.gpu_num:
                x_batch = x_batch.cuda()
            output = self.model(x_batch)
            output = nn.functional.softmax(output, dim=-1)
            if self.args.gpu_num:
                output = output.cpu()
            outputs.extend(output.tolist())
        return np.array(outputs)

    def main(self):
        if self.args.local_rank == 0: self.logger.info("start training")

        for k in range(self.config.K_FOLD):
            # 重置模型
            self.model = self.build()
            self.initialize()

            if self.args.local_rank == 0: self.logger.info("Now is training on fold-{}".format(k + 1))
            for epoch in range(self.config.NUM_EPOCHS):
                if self.args.local_rank == 0: self.logger.info("epoch {} / {} :".format(epoch + 1, self.config.NUM_EPOCHS))
                self.train(self.loader.trains[k])
            if self.args.local_rank == 0: self.logger.info("Training on fold-{} finished, now predicting cross-validated data".format(k + 1))
            np.save(os.path.join(self.config.SAVE_FOLD, "fold_{}_probs.npy".format(k + 1)), self.eval(self.loader.holdouts[k]))
            if self.args.local_rank == 0: self.logger.info("Successfully saved prediction of fold {}".format(k + 1))

    def get_label_errors(self):
        # 合并每一个fold中的holdout结果即整个数据集的结果

        if self.args.local_rank == 0: self.logger.info("Start combining all probability fold")
        data = []
        all_probs = np.empty((0, self.config.CLASS_NUM))

        for k in range(self.config.K_FOLD):
            with open(os.path.join(self.config.SAVE_FOLD, 'holdout_fold_{}.txt'.format(k + 1))) as f:
                data += f.readlines()
            probs = np.load(os.path.join(self.config.SAVE_FOLD, "fold_{}_probs.npy".format(k + 1)))
            all_probs = np.append(all_probs, probs, axis=0)

        with open(os.path.join(self.config.SAVE_FOLD, 'holdout_fold_all.txt'), 'w+', encoding='utf-8') as f:
            print(''.join(data), file=f)

        labels = [line.rstrip().split('\t')[-1] for line in data]
        numpy_array_of_noisy_labels = np.array([self.config.LABELS.index(label) for label in labels])

        ordered_label_errors = get_noise_indices(
            s=numpy_array_of_noisy_labels,
            psx=all_probs,
            sorted_index_method='prob_given_label',
            prune_method='prune_by_noise_rate'
        )

        with open(os.path.join(self.config.SAVE_FOLD, 'ordered_label_errors.txt'), 'w+', encoding='utf-8') as f:
            print('\n'.join(map(lambda x: str(x), ordered_label_errors)), file=f)
        if self.args.local_rank == 0: self.logger.info("Successfully combined all probability fold")


if __name__ == '__main__':
    warnings.filterwarnings("ignore")
    args = train_parser()
    args.gpu_num = torch.cuda.device_count()

    if args.gpu_num > 1:
        torch.distributed.init_process_group(backend="nccl")
        args.local_rank = torch.distributed.get_rank()
        torch.cuda.set_device(args.local_rank)
        device = torch.device("cuda", args.local_rank)

    config = CrossValTrainConfig(args)
    clf = MultiClassifier(args, config)
    clf.main()
    clf.get_label_errors()
