# 配置文件内容示例
所需自定义的配置文件如下：
- config.json：主要配置文件，包含依赖表名，各级输出表名配置等。
- col_config.json：实体顺序以及dwb清洗配置等。
- domain_extractor.py：目标数据获取函数自定义代码文件。
- dict_list_file.json：附加字典表，可用于目标数据获取使用自定义字典表，以及规则分类等字典表。
- clean_udf.py：自定义清洗UDF函数代码文件。
- class_run.sh：模型分类容器运行脚本。
- ner_run.sh：实体提取容器运行脚本。
## config.json 配置文件
该配置文件为主要配置
涉及到：源数据表，分类表名，实体提取表名，dwb表名，HDFS中转基路径，分类模型版本名称，实体提取模型版本名称，实体提取模型名称，以及附属配置等。

## col_config.json 配置文件
该配置文件为实体字段及dwb清洗字段配置，示例如下：
```json
{
    "金融_银行_储蓄账户_收入": {
        "ner_list": [
            "交易对象卡号(尾号)",
            "交易金额币种",
            "交易金额",
            "交易对象",
            "余额",
            "卡号(尾号)",
            "余额币种",
            "姓名"
        ],
        "ner2nlp_col": {
            "交易对象卡号(尾号)": "other_card_number",
            "交易金额币种": "transaction_amount_unit",
            "交易金额": "transaction_amount",
            "交易对象": "other_name",
            "余额": "balance",
            "卡号(尾号)": "self_card_number",
            "余额币种": "balance_unit",
            "姓名": "self_name"
        },
        "nlp2dwb": {
            "other_card_number": [
                "other_card_number",
                "clean_card_number_udf"
            ],
            "transaction_amount_unit": [
                "transaction_amount_unit",
                "amount_unit_udf"
            ],
            "transaction_amount": [
                "transaction_amount",
                "amount_udf"
            ],
            "other_name": [
                "other_name",
                "base_udf"
            ],
            "balance": [
                "balance",
                "amount_udf"
            ],
            "self_card_number": [
                "self_card_number",
                "clean_card_number_udf"
            ],
            "balance_unit": [
                "balance_unit",
                "amount_unit_udf"
            ],
            "self_name": [
                "self_name",
                "name_udf"
            ],
            "type": [
                "msg",
                "income_transaction_type_udf"
            ]
        }
    }
}
```
- `ner_list`  
    字段配置的为实体顺序，需要和ner模型文件中的tag.json实体顺序一致。  
- `ner2nlp_col`  
    字段配置的为实体提取的实体名称到表字段名的映射。  
- `nlp2dwb`  
    字段配置的是dwb清洗字段名以及依赖的字段，使用的udf函数等。  
    ```json
    "other_card_number": [
        "other_card_number",
        "clean_card_number_udf"
    ]
    ```
    其中`other_card_number`为清洗后dwb表字段名，列表的第一个值`other_card_number`为依赖的数据，可以是实体提取的字段，也可以是原始msg，可以并列多个，比如`other_card_number,msg`等。列表的第二个值`clean_card_number_udf`是清洗该字段使用的自定义UDF函数名，该函数的具体定义在`clean_udf.py`中定义。

## domain_extractor.py 自定义目标数据获取代码
在该自定义代码文件中，需要实现：`domain_extractor`函数，函数示例如下：
```python
def domain_extractor(msg, app_name, suspected_app_name, hashcode, abnormal_label, dict_list={}):
    """
    参数：msg,app_name,suspected_app_name,hashcode,abnormal_label皆为原始数据表中的字段，dict_list为附加字典表load后的字典对象
    返回值：True（获取该条数据）或者False（不要该条数据）
    """
```
参数列表及返回值如注释所示
## clean_udf.py 自定义清洗函数代码
在该自定义代码文件中，需要创建自定义UDF字典对象：`clean_udf_dict`。  
在该字典对象中，可配置输入参数为1，2，3个的自定义UDF函数。示例如下：
```python
clean_udf_dict = {
    'clean_udf_dict1': {
        "name_udf": name_clean
        "amount_udf": amount_udf,
        "loan_type_udf": loan_type_udf,
        "clean_card_number_udf": clean_card_number_udf,
        "income_transaction_type_udf": income_transaction_type_udf,
        "outcome_transaction_type_udf": outcome_transaction_type_udf,
        "amount_unit_udf": amount_unit_udf,
        "date_clean_udf": date_clean,
        "base_udf": base_udf,
        "status_udf": status_udf,
        "prepayment_udf": prepayment_udf
    }
}
```
其中子key可选值为：`clean_udf_dict1`,`clean_udf_dict2`,`clean_udf_dict3`,分别代表可传入参数数量为1个，2个，3个。  