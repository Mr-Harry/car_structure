# **Template Monitoring 模版监控平台**

## 1. 系统功能介绍
实现对输入数据的监控，摸清数据的模板分布，定时监控T-13天的模板变化


## 2. 主要依赖包
- Python3
- PySpark 2.0+
- Numpy 1.17.0+
- yaml
- graphframes
- 



## 2. 详细使用说明

### 2.0 定时监控
```
sh start.sh
```

### 2.1 全类别增量高频模板按天聚类  
```
sh run_final.sh `start_date` `end_date` `config_file`
参数实例：  
  start_date=20210101
  end_date=20210630 
  config_file=yamls/config_final.yaml
```
全类别增量高频模板按天聚类中，主要包括3个任务：
+ cluster.run_final_increment(`the_date`): 计算增量数据的hash模板
+ cluster.run_final_tmp(`the_date`, `classified_fun`): 对模板数据进行行业分类，并根据hashcode将模板数据和历史数据进行聚合
+ cluster.write_base(): 根据tmp_table.query，获取聚合后的高频数据，并写入base表。

### 2.2 单独行业增量高频数据按周聚类
```
sh run_industry.sh `start_date` `end_date` `config_file`
参数实例：  
  start_date=20210104                          # 当开始日期不是周一的话，程序会自动以下个周一为开始日期
  end_date=20210630                            # 当结束日期不是周日的话，程序会自动以上个周日为结束日期
  config_file=yaml/config_industry_bank.yaml  
```
单独行业增量高频数据按周聚类，主要流程包括：
+ cluster.run_industry_increment(`class_label`, `monday`, `sunday`): 从全类别的高频模板数据中获取`monday`到`sunday`内行业包含`class_label`的增量高频数据（取前98%的高频模板）
+ python3 action_classifier/action_classifier.py --config_file `config_name` : 对高频增量数据采用模型分类，输出行业行为
+ cluster.run_industry_tmp(`anchor`, `monday`, `sunday`): 采用Minhash+Union_Find算法对增量数据进行进一步的聚类，并将结果写入tmp表
+ cluster.write_base(): 根据tmp_table.query，获取聚合后的高频数据，并写入base表
  
### 2.3 源头数据质量监控
该部分主要为统计相应的数据，上传到MySQL，便于调用商业智能系统进行直观的数据质量展示
```
sh data_BI/DataBI.sh --config_file data_BI/bi_config/final.yaml --task_type final   # 全量监控  
sh data_BI/DataBI.sh --config_file data_BI/bi_config/bank.yaml --task_type industry # 分行业监控  
```
源头数据质量监控，存在两方面的监控：
+ 对全量数据的监控:  监控表中：每天的数据量（全类别、各类别），模板量（分类别），高频模板量（分类别）
+ 对行业数据的监控:  监控表中每周的模板量，每周模板增加量及每周模板减少量

### 2.4 结构化任务数据采样
结构化任务需要进行数据标注，可通过对采样表进行采样-聚类得到需标注的数据
+ 1. 完成nlp_structure_config中相应类目的数据获取代码data_extractor.py 和 dict_list_file.json，并将nlp_structure_config命名为config_sample
  ```
  git clone http://10.1.21.81:20011/teletubbies/nlp_structure_config.git
  mv nlp_structure_config config_sample
  vim config_sample/`config_name`/data_extractor.py config_sample/`config_name`/dict_list_file.json   # 若是新项目，需按其他项目形式创建config_name项目文件夹
  ```
+ 2. 生成相应的表格
  ```
  sh sample_hash/create_sql.sh `app_name` `config_name`
  ```
+ 3. 进行采样及聚类
  ```
  sh sample_hash/start.sh  `app_name` `config_name` `start_month` `end_month` `sample_ratio`
  ```
注释：app_name为项目的描述，config_name为项目名称，start_month一般为202001， end_month为已跑完采样的最近月份，sample_ratio为采样率；


## 3. 使用反馈
1. 后续可增加：全类别增量模板的模型分类，代码和行业分类基本一致
2. 数据来源为采样表
3. 结构改为全量分区表，保留字段包括：first_msg, c_id, cnt, hashcode, class_label, class_label_prob, industry_label, 类特征（word_bag, ）, the_date
   1. 生成增量聚类数据，无C_ID 
   2. 增量聚类数据join全量表，生成相应C_id
   3. 将增量数据插入全量新分区
4. 行业聚合后放按行业分区写入行业表，不另外建表
5. 