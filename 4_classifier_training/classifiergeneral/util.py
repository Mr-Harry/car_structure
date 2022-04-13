# coding:utf-8
import logging
import sys
import argparse


class Type(object):
    @classmethod
    def str(cls):
        raise NotImplementedError


class ModelType(Type):
    TEXTCNN = "TEXTCNN"
    DPCNN = "DPCNN"
    BIRNN = "BIRNN"
    TransformerLC = "TransformerLC"
    fasttext = "fasttext"
    VDCNN = "VDCNN"
    SVDCNN = "SVDCNN"

    @classmethod
    def str(cls):
        return ",".join([cls.TEXTCNN, cls.DPCNN, cls.BIRNN, cls.TransformerLC, cls.fasttext, cls.VDCNN, cls.SVDCNN])


class Logger(object):
    _instance = None

    def __new__(cls, *args, **kw):
        if not cls._instance:
            cls._instance = super(Logger, cls).__new__(cls)
        return cls._instance

    def __init__(self, config):
        if config.LOG_LEVEL == "debug":
            logging_level = logging.DEBUG
        elif config.LOG_LEVEL == "info":
            logging_level = logging.INFO
        elif config.LOG_LEVEL == "warning":
            logging_level = logging.WARNING
        elif config.LOG_LEVEL == "error":
            logging_level = logging.ERROR
        elif config.LOG_LEVEL == "critical":
            logging_level = logging.CRITICAL
        else:
            raise TypeError(
                "No logging type named %s, candidate is: info, debug, error")
        logging.basicConfig(filename=config.LOG_FILE,
                            level=logging_level,
                            format='%(asctime)s : %(levelname)s  %(message)s',
                            filemode="a", datefmt='%Y-%m-%d %H:%M:%S')

    @staticmethod
    def debug(msg):
        """Log debug message
            msg: Message to log
        """
        logging.debug(msg)
        # sys.stdout.write(msg + "\n")

    @staticmethod
    def info(msg):
        """"Log info message
            msg: Message to log
        """
        logging.info(msg)
        # sys.stdout.write(msg + "\n")

    @staticmethod
    def warn(msg):
        """Log warn message
            msg: Message to log
        """
        logging.warning(msg)
        # sys.stdout.write(msg + "\n")

    @staticmethod
    def error(msg):
        """Log error message
            msg: Message to log
        """
        logging.error(msg)
        # sys.stderr.write(msg + "\n")
    
    @staticmethod
    def critical(msg):
        logging.critical(msg)
        # sys.stderr.write(msg + "\n")


def basic_parser(parser):
    pass
    

def train_parser():
    parser = argparse.ArgumentParser("Train Argument")
    parser.add_argument("--train_dataset_path", type=str, help="训练集路径")
    parser.add_argument("--model_name", type=str, help="模型保存名称")
    parser.add_argument("--label_path", type=str, default="config/label.json", help="标签路径")
    parser.add_argument("--train_rate", type=float, default=0.8, help="训练集比例")
    parser.add_argument("--validate_rate", type=float, default=0.2, help="验证集比例")
    parser.add_argument("--test_rate", type=float, default=0, help="测试集比例")
    parser.add_argument("--log_level", type=str, default="info", help="log日志输出等级，可选(debug, info, warning, error, critical)")
    parser.add_argument("--log_file", type=str, default=None, help="log日志输出文件")
    parser.add_argument("--model_method", type=str, default="DPCNN", help="选用模型方法(TEXTCNN, DPCNN, BIRNN, fasttext, TransformerLC, VDCNN, SVDCNN)")
    parser.add_argument("--sep", type=str, default='\t', help="训练数据中文本和标签之间的分隔符")
    parser.add_argument("--seed", type=int, default=2020, help="随机种子")
    parser.add_argument("--unsample_num", type=int, default=0, help="上采样最小数据,0则为不采样")
    parser.add_argument("--optimizer", type=str, default="Adam", help="优化器(Adam,SGD,RMSprop)")
    parser.add_argument("--epoch", type=int, default=50, help="训练轮数")
    parser.add_argument("--batch_size", type=int, default=256)
    parser.add_argument("--is_embedding", action='store_false', help="是否加入EMBEDDING层")
    parser.add_argument("--embedding_dim", type=int, default=300, help="Embedding层的维度")
    parser.add_argument("--max_len", type=int, default=140, help="每条文本选取的最大长度,推荐140*n_gram")
    parser.add_argument("--num_workers", type=int, default=6, help="数据生成器进程数")
    parser.add_argument("--half", action='store_true', help="是否使用混合精度，加上该参数则为是")
    parser.add_argument("--weighted_class", action='store_true', help="是否对loss function加上class weight")
    parser.add_argument("--local_rank", type=int, default=0, help='pytorch分布式训练默认参数')
    parser.add_argument("--n_gram", type=int, default=1, help="n-gram的n值，fasttext建议设为3")
    parser.add_argument("--vocab_max_num", type=int, default=100000, help="vocab最大词汇数")
    parser.add_argument("--pad_direction", type=str, default="right", help="补零方向，默认向右")
    parser.add_argument("--pretrained", type=str, default=None, help="预备训练向量路径")
    parser.add_argument("--k_fold", type=int, default=4, help="K-Fold交叉验证折数")
    args = parser.parse_args()
    return args

def validate_parser():
    parser = argparse.ArgumentParser(description="Predict Argument")
    parser.add_argument("--model_fold", type=str, help="参数文件夹（模型，字映射字典等）")
    parser.add_argument("--log_level", type=str, default="info", help="log日志输出等级，可选(debug, info, warning, error, critical)")
    parser.add_argument("--log_file", type=str, default=None, help="log日志输出文件")
    parser.add_argument("--model_method", type=str, default="DPCNN", help="选用模型方法(TEXTCNN, DPCNN, BIRNN, fasttext, TransformerLC, VDCNN, SVDCNN)")
    parser.add_argument("--batch_size", type=int, default=2048)
    parser.add_argument("--half", action='store_true', help="是否使用混合精度")
    parser.add_argument("--read_sep", type=str, default="\t", help="read file separator")
    parser.add_argument("--write_sep", type=str, default="\t", help="write file separator")
    parser.add_argument("--msg_index", type=int, default=4, help="the index of msg after split text")
    parser.add_argument("--read_path", type=str, help="file read path")
    parser.add_argument("--write_path", type=str, help="file write path")
    parser.add_argument("--pretrained", type=str, default=None, help="预备训练向量路径")
    args = parser.parse_args()
    return args

def predict_parser():
    parser = argparse.ArgumentParser(description="Predict Argument")
    parser.add_argument("--model_fold", type=str, help="参数文件夹（模型，字映射字典等）")
    parser.add_argument("--log_level", type=str, default="info", help="log日志输出等级，可选(debug, info, warning, error, critical)")
    parser.add_argument("--log_file", type=str, default=None, help="log日志输出文件")
    parser.add_argument("--model_method", type=str, default="DPCNN", help="选用模型方法(TEXTCNN, DPCNN, BIRNN, fasttext, TransformerLC, VDCNN, SVDCNN)")
    parser.add_argument("--batch_size", type=int, default=2048)
    parser.add_argument("--num_workers", type=int, default=6, help="数据生成器进程数")
    parser.add_argument("--half", action='store_true', help="是否使用混合精度")
    parser.add_argument("--read_sep", type=str, default="\t", help="read file separator")
    parser.add_argument("--write_sep", type=str, default="\t", help="write file separator")
    parser.add_argument("--msg_index", type=int, default=4, help="the index of msg after split text")
    parser.add_argument("--local_rank", type=int, default=0, help='pytorch分布式训练默认参数')
    parser.add_argument("--read_path", type=str, help="file read path")
    parser.add_argument("--write_path", type=str, help="file write path")
    parser.add_argument("--pretrained", type=str, default=None, help="预备训练向量路径")
    args = parser.parse_args()
    return args
