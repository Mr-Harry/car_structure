import re

def domain_extractor(msg, app_name, suspected_app_name, hashcode, abnormal_label, dict_list={}):
    """
    参数：msg,app_name,suspected_app_name,hashcode,abnormal_label皆为原始数据表中的字段，dict_list为附加字典表load后的字典对象
    返回值：True（获取该条数据）或者False（不要该条数据）
    """
    rule_out = r'险|租|电|房|装修|教育|乘*车[票费]|司机|移车|ETC|资讯|证券|股|基金|涨|跌|卖|[约专快打租单托拖运拼风叫机停泊有用行客货派公中吊卡重叉通出务动力]车|货运|验证码|登陆凭证|动态密码|激活码|取件码|退订|回复*[a-zA-Z]{1,4}[退拒]|拒收回[a-zA-Z]{1,4}|点击|戳|订回[a-zA-Z]{1,4}|低至.折'
    rule_in_1 = r'车'
    rule_in_2 = r'汽车'
    rule_in_3 = r'(?<![约专快打租单托拖运拼风叫机停泊有用行客货派公中吊卡重叉通出校务动力])车$'
    rule_in_4 = r'看车|试驾|试乘|提车|交易车辆|买车|购车|支付.*车款|售车|车抢购|购得爱车|喜得爱车|金融方案|约看(?![牙诊])|带看|询价|(询问|咨询).*价格|咨询.*销售顾问|欲购买|摇号|指标|竞价|竞得|竞拍|上拍|流拍|开拍|拍卖|流拍|报价|出价|成交|撮合邀请|感谢您来到'

    if abnormal_label != '正常文本':
        return False
    # 过滤截断的短文本, 过滤msg的排除规则
    if len(re.sub('[^\u4e00-\u9fa5]','',msg))>15 and not re.search(rule_out,msg):
        if (re.search(rule_in_2,app_name) \
        or re.search(rule_in_3,app_name) \
        or re.search(rule_in_2,suspected_app_name) \
        or re.search(rule_in_3,suspected_app_name) \
        or (re.search(rule_in_1,msg) and re.search(rule_in_4,msg))):
            return True
        else:
            return False
    else:
        return False


