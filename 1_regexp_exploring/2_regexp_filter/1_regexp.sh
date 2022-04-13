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
app_name regexp '汽车' OR
app_name regexp '(?<![约专快打租单托拖运拼风叫机停泊有用行客货派公中吊卡重叉通出校务动力])车$' OR
suspected_app_name regexp '汽车' OR
suspected_app_name regexp '(?<![约专快打租单托拖运拼风叫机停泊有用行客货派公中吊卡重叉通出校务动力])车$' OR
(
msg regexp '车' AND
msg regexp '看车|试驾|试乘|提车|交易车辆|买车|购车|支付.*车款|售车|车抢购|购得爱车|喜得爱车|金融方案|约看(?![牙诊])|带看|询价|(询问|咨询).*价格|咨询.*销售顾问|欲购买|摇号|指标|竞价|竞得|竞拍|上拍|流拍|开拍|拍卖|流拍|报价|出价|成交|撮合邀请|感谢您来到'
)
) AND
(
msg not regexp '险|租|电|房|装修|教育|乘*车[票费]|司机|移车|ETC|资讯|证券|股|基金|涨|跌|卖' AND
msg not regexp '[约专快打租单托拖运拼风叫机停泊有用行客货派公中吊卡重叉通出务动力]车|货运' AND
msg not regexp '验证码|登陆凭证|动态密码|激活码|取件码' AND
msg not regexp '退订|回复*[a-zA-Z]{1,4}[退拒]|拒收回[a-zA-Z]{1,4}|点击|戳|订回[a-zA-Z]{1,4}|低至.折'
)
;
"

cd /home/${rss_path}/ && bash rss.sh "data_explorer" "nlp_dev" "$sql_part"

if [[ $? != 0 ]];then
echo "sql 运行失败！！！！！！"
exit 1
fi
echo 数据写入完成

