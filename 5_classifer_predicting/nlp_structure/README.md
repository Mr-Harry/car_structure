# nlp_structure
结构化生产流程代码

# 目录结构
+ domain/:  目标数据获取模块
+ classifier/: 数分类模块
+ ner/: 实体提取模块
+ cleaner/: 数据清洗模块
+ config/: 自定义配置。
+ tool/: 辅助工具包

# 项目整体逻辑
+ 使用目标数获取模块进行目标数据获取，产出目标数据表。
+ 使用数据分类模块进行数据分类工作，产出分类成果表。
+ 使用实体提取模块进行实体提取工作，产出各类目的实体成果表；
+ 使用数据清洗模块进行数据清洗工作，产出各类目的事件库成果表；

# 项目使用说明
本项目配合自定义配置项一起使用  
将自定义配置文件夹移动到`config`文件夹下
- 目标数据获取：  
`python3 domain/domain.py --config_name config_example --the_date 2020-10-01 --file_no merge_20201009_10531_L1_rule_merge --config_type test`
- 数据分类：  
`python3 classifier/classifier.py --config_name config_example --the_date 2020-10-01 --file_no merge_20201009_10531_L1_rule_merge --config_type test`  
- 实体提取：  
`python3 ner/ner.py --config_name config_example --the_date 2020-10-01 --file_no merge_20201009_10531_L1_rule_merge --config_type test`  
- 数据清洗：  
`python3 cleaner/cleaner.py --config_name config_example --the_date 2020-10-01 --file_no merge_20201009_10531_L1_rule_merge --config_type test`
## 自定义配置
自定义配置用于配置具体结构项目中用到的各种配置项和外置代码，例如目标数据获取，清洗等。具体所需配置如下：
- config_test.json：主要配置文件，包含依赖表名，各级输出表名配置等。 线上运行时需增加config_master.json和config_customer.json
- col_config.json：实体顺序以及dwb清洗配置等。
- domain_extractor.py：目标数据获取函数自定义代码文件。
- dict_list_file.json：附加字典表，可用于目标数据获取使用自定义字典表，以及规则分类等字典表。
- clean_udf.py：自定义清洗UDF函数代码文件。
- class_run.sh：模型分类容器运行脚本。
- ner_run.sh：实体提取容器运行脚本。  

## 新增
### 新增配置文件类型
为了测试，客户测试，生产等所有该项目下的目标数据获取，分类，实体识别，清洗等代码即流程代码相同，但又要配置不同的输入输出表，增加了配置文件类型的使用方法。默认所有流程使用的是`master`后缀的输入输出表配置文件，如有需要配置测试，客户测试等不同的输入输出表，可以新增不同后缀的`config`文件。

### 新增dwb清洗规则过滤
在`col_config.json`文件中，类目下增加`rule_filter`项，方式和自定义UDF清洗函数类似。样例如下：
```json
"nlp2dwb":{……},
"rule_filter": [
            "msg",
            "credit_overdue_udf"
        ]
```
该自定义UDF函数可以使用jar包上传的UDF函数，也可以使用在`clean_udf.py`里自定义的。需要注意的是，该自定义UDF函数需要返回字符串类型的`true`或者`false`。

### 新增dwb清洗指定单表测试
在dwb清洗启动时，增加`--test_class_name`参数，可以选择仅清洗该表。示例如下：
```bash
python3 cleaner/cleaner.py --config_name config_example --the_date 2020-10-01 --file_no merge_20201009_10531_L1_rule_merge --test_class_name 金融_银行_信用卡_逾期
```

### 新增ner跑数指定类目
在ner启动时，增加`--ner_dict`参数，可以制定类目进行NER跑数。示例如下：
```bash
python3 ner/ner.py --config_name config_example --the_date 2020-10-01 --file_no merge_20201009_10531_L1_rule_merge --ner_dict 金融_银行_信用卡_逾期
```

具体实例参见`config_example`文件夹下的内容