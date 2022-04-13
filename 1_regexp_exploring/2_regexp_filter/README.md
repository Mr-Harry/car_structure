# 正则数据流
本数据流的功能：
1. 按照抽样表的表结构建新表
2. 利用自定义的正则过滤抽样表，结果写入新表

在本数据流中
抽样表是`nlp_dev.qianyu_20220318`
新表是`nlp_dev.qianyu_20220318_car`
最终的小表的数据，可以通过如下命令，以txt格式在本地路径(服务器)生成
```bash
hdfs dfs -cat 'hdfs://fcycdh/user/hive/warehouse/nlp_dev.db/qianyu_20220318_car/the_date=2021-12-02/file_no=merge_20211202_1202_L0/part-00000-40630543-8f11-4d10-aa7c-76502e02c7ec.c000'>log.out
# 将黑框输出的内容重定向到日志文件log.out
```
## 0_create_table.sh
```bash
#!/bin/bash
source /etc/profile
source ~/.bash_profile

source_database_name=$1
source_table_name=$2
new_database_name=$3
new_table_name=$4

sql_part="
drop table if exists ${new_database_name}.${new_table_name};
create table ${new_database_name}.${new_table_name} like ${source_database_name}.${source_table_name};
alter table ${new_database_name}.${new_table_name} set fileformat textfile;
alter table ${new_database_name}.${new_table_name} set serdeproperties('serialization.format'='\t', 'field.delim'='\t');
"

beeline -u "jdbc:hive2://coprocessor01-fcy.hadoop.dztech.com:2181,coprocessor02-fcy.hadoop.dztech.com:2181,coprocessor03-fcy.hadoop.dztech.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "$sql_part"
if [[ $? != 0 ]];then
echo "sql 运行失败！！！！！！"
exit 1
fi
echo 建表完成
```
脚本逻辑
1. 建表sql的代码逻辑
- 修改文件存储类型(set fileformat)
- 修改列分隔符(set serdeproperties)
- 旧分区要drop掉

2. beeline的命令行的脚本逻辑
- -u参数，连接hive
- -e参数，sql代码

3. shell脚本的if判断逻辑
- 如果代码运行失败，输出："sql 运行失败！！！！！！"
- 如果代码运行成功，输出：建表完成

## 1_regexp.sh
```bash
#!/bin/bash
source /etc/profile
source ~/.bash_profile

rss_path=$1
source_database_name=$2
source_table_name=$3
new_database_name=$4
new_table_name=$5

sql_part="
#!/bin/bash
source /etc/profile
source ~/.bash_profile

rss_path=$1
source_database_name=$2
source_table_name=$3
new_database_name=$4
new_table_name=$5

sql_part="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${new_database_name}.${new_table_name} partition(the_date,file_no) select * from ${source_database_name}.${source_table_name} where 
abnormal_label regexp '正常文本' AND
(
msg regexp '(?<![约专快打租单托拖运拼风叫机停泊有用行客货派])车' OR
app_name regexp '(?<![约专快打租单托拖运拼风叫机停泊有用行客货派])车$' OR
suspected_app_name regexp '(?<![约专快打租单托拖运拼风叫机停泊有用行客货派])车$'
) AND
(
msg not regexp '移动彩印提醒|温馨提醒|共抗疫情|来自\d*|车险|交强险|投保|商业险|保险|强险|保费|车损|三者|首保|车船税|车船税|交强|财险|续保|机动车损失险|责任险|事故损失车辆|定损|理赔|报价单|报价|天气|[买购]房|房屋按揭|贷款装修|信用|征信|结婚|留学|进修|教育|旅游|飞机|电动车|摩托车|公务用车|卡车|叉车|大卡|吊车|升降车|物流车|验证码|登陆凭证|诈骗|动态密码|开通亲情网|司机|叫车|快车|专车|出租|顺风车|拼车|车票|雅迪|老板|车商|线索|待回访客户|直通车|待跟进意向|及时联系客户|餐厅|车间|员工|服务请求|警情号|公车|用车人|柴油|退订|回[a-zA-Z]退|回[a-zA-Z][a-zA-Z]退|拒收回[a-zA-Z]|点击立即领取|点击链接|诚挚来电|恭喜您|瓜分|积分奖励|推荐|详情|违章|违规|违法|交警|超速|限速|乘用车|发货|租赁|儿童车|证券|获利|短线|冲高|投资|止盈点|基金|大盘|美股|港股|指数|理财|珍爱网|红娘|已购车房|购车有房|购车购房|男士情况|男士条件|金联创|路况信息|车费|合伙人|好友申请|公安厅|绑定|登录|注册|官方旗舰店|小灵狗出行|车位|[0-9]*股|[0-9]*吨|银行客户|贷款逾期案件|失信执行人名单|黑名单|业主|维修|放假|面试|入职申请|学校|淘宝|客服|师傅|接单|中信建投|连信|快手科技|告警|节点|模块|CPU|接口|测试|集群|孩子|校门口|老师|家长|考试|借款申请|激活码' AND
app_name not regexp '哈罗|哈啰|滴答|滴滴|流量|共享.*车|自行车|助力车|公务用车|公务车|旅行车|外卖|公车|出警|中车|卡车|叉车|大卡|吊车|升降车|证券|基金|法院|法律|信用卡|公安[厅局]|银行|资讯|重汽|电商|汽车金融' AND
suspected_app_name not regexp '哈罗|哈啰|滴答|滴滴|流量|共享.*车|自行车|助力车|公务用车|公务车|旅行车|外卖|公车|出警|中车|卡车|叉车|大卡|吊车|升降车|证券|基金|法院|法律|信用卡|公安[厅局]|银行|资讯|重汽|电商|汽车金融'
)
;
"

cd /home/${rss_path}/ && bash rss.sh "data_explorer" "nlp_dev" "$sql_part"

if [[ $? != 0 ]];then
echo "sql 运行失败！！！！！！"
exit 1
fi
echo 数据写入完成
"

cd /home/${rss_path}/ && bash rss.sh "data_explorer" "nlp_dev" "$sql_part"

if [[ $? != 0 ]];then
echo "sql 运行失败！！！！！！"
exit 1
fi
echo 数据写入完成
```
脚本逻辑
1. `set hive.exec.dynamic.partition.mode=nonstrict;`这个属性默认值是strict,就是要求分区字段必须有一个是静态的分区值，当前设置为nonstrict,那么可以全部动态分区
2. 正则的逻辑：首先保留msg字段含有“车”的短文本，然后过滤app_name,suspected_app_name,msg字段含过滤关键字的短文本
3. 切换到`/home/nlp_dev`路径，运行`rss.sh`脚本，使用spark_sql跑插入语句的sql代码
4. shell脚本的if判断逻辑
- 如果代码运行失败，输出："sql 运行失败！！！！！！"
- 如果代码运行成功，输出：建表完成

正则的逻辑
- 正向正则圈选：
    - msg包含“车”,，约车，专车，快车，打车等不能匹配 OR
    - app_name`*车`结尾，约车，专车，快车，打车等不能匹配 OR
    - suspected_app_name`*车`结尾，约车，专车，快车，打车等不能匹配 
- AND
- 反向正则过滤：
    - msg不包含，移动彩印提醒，温馨提醒，共抗疫情等 AND
    - app_name不包含，哈罗，哈啰，滴答，滴滴等 AND
    - suspected_app_name不包含，哈罗，哈啰，滴答，滴滴等

正则优化目标
我想要的汽车数据应该是这样子的：
- 体现出有车需要维修保养的人群
- 体现出又购车意愿的人群
共同点：想要交钱消费的人群

## run.sh
```bash
#!/bin/bash -e
baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)
bash ${baseDirForScriptSelf}/0_create_table.sh nlp_dev qianyu_20220318 nlp_dev qianyu_20220318_car
bash ${baseDirForScriptSelf}/1_regexp.sh nlp_dev nlp_dev qianyu_20220318 nlp_dev qianyu_20220318_car
```
脚本逻辑
1. 变量`baseDirForScriptSelf`是当前脚本所在的路径
2. 串联了0_create_table.sh和1_regexp.sh两个脚本的运行
