**General Classifier**
===

# 1. 介绍（Introduction）
该项目目的是打造一个通用的文本分类器，它建立在[Pytorch](https://pytorch.org/)上，目前已实现fasttext, TEXTCNN, DPCNN, BIRNN, TransformerLC。

# 2. 主要依赖包（Requirement）
- python 3
- Pytorch 1.0+
- Numpy 1.17.0+

# 3. 用法（Usage）
## 3.1 数据集生成
文本在前标签在后，无须标题（即第一行就是原始数据），分隔符默认为`\t`(可在参数中修改)；建议创建一个`dataset`文件夹然后训练集放入其中。

## 3.2 配置文件
标签放在`config/label.json`文件中，格式如下
```json
[
    "Label_1",
    "Label_2",
    "Label_3",
    "...",
]
```
阈值放在`config/threshold.json`文件中，格式如下
```json
{
    "label_1": 0.0,
    "label_2": 0.1,
    "...": 0.0
}
```

## 3.3 训练
> 默认混合精度，加上`--half`使用单精度，具体可用`python train.py -h`查看参数
- 多卡
```bash
CUDA_VISIBLE_DEVICES=0,1 OMP_NUM_THREADS=1 python -m torch.distributed.launch --nproc_per_node=2 --master_addr="localhost" --master_port=23456 train.py --train_dataset_path * --model_name * --half
```
其中 `--nproc_per_node=2` 表示可用的GPU的数量，注意`master_port`需满足同机不同端口号
```bash
# 示例：使用fasttext 3-gram 训练
CUDA_VISIBLE_DEVICES=0,1 OMP_NUM_THREADS=1 python -m torch.distributed.launch --nproc_per_node=2 --master_addr="localhost" --master_port=23456 train.py --train_dataset_path  dataset/train.txt --model_name fasttext_train --half --model_method fasttext --n_gram 3 --max_len 420 --batch_size 256 --epoch 50
```


- 单卡
```bash
CUDA_VISIBLE_DEVICES=0 python train.py --train_dataset_path * --model_name *
```

## 3.4 预测
```
CUDA_VISIBLE_DEVICES=0,1 OMP_NUM_THREADS=1 python -m torch.distributed.launch --nproc_per_node=2 multiple_predict.py --model_fold * --read_path * --write_path * --msg_index * --half
```

## 3.5 脏数据处理
```bash
CUDA_VISIBLE_DEVICES=0,1 OMP_NUM_THREADS=1 python -m torch.distributed.launch --nproc_per_node=2 --master_addr="localhost" --master_port=23456 train_crossval.py --train_dataset_path * --model_name * --half
```
model_name下会生成`holdout_fold_all.txt`和`ordered_label_errors.txt`两个文件，`ordered_label_errors.txt`中每一行出现的数字代表`holdout_fold_all.txt`中有问题的数据的行号（从0开始）
