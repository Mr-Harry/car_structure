# 推理代码仓库
该仓库里主要包含推理Work相关代码，用于搭配使用Rust构建的其它推理组件。
## 主要目录结构
- predict
  主要包含兼容分类模型的相关代码
- utils
  工具类代码库，和NER仓库，任务调度仓库共用。
- predict_worker_classify.py
  分类推理Work启动脚本
- predict_worker.py
  NER推理Work启动脚本
- start_predict_worker.sh
  NER推理启动Shell脚本
- start_predict_worker_classfiy.sh
  分类推理启动Shell脚本

## 使用说明
该项目无法单独使用，需要配合NER仓库代码和分类仓库代码。具体如下：
### NER
需要将NER仓库代码放置在本仓库下，命名为`ner`，并建立ner配置文件夹，命名为`ner_config`，文件夹内按照NER类目存放，每个类目下存放一个`json`格式的文件名-模型名的映射文件，一个`saved_model`文件夹，其中存放每个模型的NER模型输出文件夹。示例如下：

- ner_config
  - bank_ner
    - bank_file.json
    - saved_model
      - cx_bilst_crf
      - xyk_zc_bilstm_crf

其中bank_file.json内容如下：

```json
[
    [
        "cx_bilst_crf",  //需要和saved_model里的模型文件夹命名一直
        [
            "银行_储蓄账户_支出",  //分类输出的文件名
            "银行_储蓄账户_收入"
        ]
    ],
    [
        "xyk_zc_bilstm_crf",
        [
            "银行_信用卡_支出"
        ]
    ]
]
```

通过`start_predict_worker.sh`脚本可以批量启动相关Work实例。参数如下：

```shell
-s 数据下发端口号"
-d 结束通知端口号"
-u 同步退出端口号"
-g 使用的GPU序号，格式为 \"1 2 3\" 表示启动三个Work进程，分别使用1 2 3号GPU,必须参数"
-m 模型存放目录"
-b batch_size大小,默认2048"
-l 日志输出目录"
```

### 分类

分类项目需要搭配分类仓库代码，和NER不太一样，需要将每个分类项目存放在该项目目录下，并增加`_classification`后缀，并确定内部的`predict.py`文件能够独立运行。并且在分类仓库目录下增加`__init__.py`文件，增加如下内容:

```python
import os
import sys

ABSOLUTE_PATH = os.path.dirname(os.path.abspath(__file__))
sys.path.append(ABSOLUTE_PATH)
```

通过`start_predict_worker_classfiy.sh`脚本可以批量启动相关Work实例。参数如下：

```shell
-s 数据下发端口号"
-d 结束通知端口号"
-u 同步退出端口号"
-g 使用的GPU序号，格式为 \"1 2 3\" 表示启动三个Work进程，分别使用1 2 3号GPU,必须参数"
-m 模型存放目录"
-b batch_size大小,默认2048"
-l 日志输出目录"
```

---

一般来说，不需要单独运行上述两个脚本，调度代码会自动启动，具体参考调度代码仓库说明。