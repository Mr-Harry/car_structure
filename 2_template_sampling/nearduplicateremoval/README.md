# **Near Duplicate Removal 模版去重工具**

## 1. 功能介绍
可以针对文本类数据的模版进行去重，具体的实现原理可以参考wiki文档：http://10.1.21.81:20012/pages/viewpage.action?pageId=106299479


## 2. 主要依赖包
- Python3
- PySpark 2.4
- Numpy 1.17.0+
- yaml
- graphframes


## 3. 详细使用说明
## 3.1 安装依赖包
上述的主要依赖包中，```pyspark```的版本需要指定在2.4的版本，```yaml```的pip索引名称是```pyyaml```。```graphframes```这个依赖包不需要安装，需要手动在根目录下创建```.ivy2/```这个文件夹然后把本仓内```env_pkgs```中的两个压缩包解压后的两个文件夹放在那个文件夹中，即

```shell
mkdir ~/.ivy2
mv env_pkgs/* ~/.ivy2/
tar xvf ~/.ivy2/cache.tgz
tar xvf ~/.ivy2/jars.tgz
```

## 3.2 使用
在```config```文件夹中找到```example.yaml```，这个是配置文件的用例，根据自己的实际情况修改这些变量和参数。需要修改以及注意的是：

```yaml
app_name: 任务名称
model: minhash
log_level: ERROR, WARN, INFO


mysql_config:
  url: 'jdbc:mysql://10.10.15.13:3306'
  mode: 'overwrite'  #'append' # 一共四个写模式，'append', 'overwrite', 'ignore', 'error', 只针对mysql的写入
  properties: {'user': 'root', 'password': 'VsjbvlpeDfkYiRCY' } # mysql的账户密码


minhash: # 如果不是很熟悉下面的参数的话，基本上这个就是最优的参数配置了
  window: 3
  permutations: 32
  bands: 4
  seed: 1201 # 如果是对增量数据进行去重，不要更改随即种子
  
  source_table:
    db_type: hive或者mysql
    query: 源表的数据查询query

  target_table:
    table_name: 目标表的表名字
    db_type: hive或者mysql
    columns: [ 'row_key', 'msg', 'app_name', 'suspected_app_name', 'hashcode', 'cnt', 'abnormal_label', 'class_label', 'hash_index_0', 'hash_index_1', 'hash_index_2', 'hash_index_3']
```

- ```app_name```改成自己自定义的名字就可以了，方便在后台查询任务运行情况
- ```log_level```三选一，推荐WARN然后在后台查询运行情况
- ```source_table```源表需要填写源表的数据库类型，```hive```或者```mysql```二选一，以及查询的query，这里的查询**必要**的列包括：
    - ```row_key```(或者任何其它uid并select as row_key）
    - ```msg```
- ```target_table```目标表的表名，数据库类型```hive```或者```mysql```二选一，以及目标表的列顺序。这里需要注意的是，目标表需要提前创建好，**必须**的列除了包括源表query中选择的列之外还需要：
    - ```cnt``` 可以是整数型值或者字符串型值
    - ```hash_index_1, hash_index_2, ... hash_index_n```每一个```hash_index```对应一个单独的列，n的数量取决于下面```minhash```的参数```bands```值。
- ```minhash```根据自己的任务需要可以修改```permutations```和```bands```两个参数
    - 默认的配置参数适用于类似于final表或者行业表（很多的app_name）这种模版很多的表
    - 如果是单一的app模版相对比较少的表，可以选择把两个参数都调小一些
        - ```permutation```可选参数```8```~```32```
        - ```bands```可选参数```2```~```4```

配置好yaml之后，就可以在本仓的根目录下运行
```shell
sh run.sh --config_file config/example.yaml
```