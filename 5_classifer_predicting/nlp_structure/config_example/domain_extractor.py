import re


def domain_extractor(msg, app_name, suspected_app_name, hashcode, abnormal_label, dict_list={}):
    """
    参数：msg,app_name,suspected_app_name,hashcode,abnormal_label皆为原始数据表中的字段，dict_list为附加字典表load后的字典对象
    返回值：True（获取该条数据）或者False（不要该条数据）
    """
    if abnormal_label != '正常文本':
        return False
    # step2: 签名符合银行行或者签名里面包含银行或者文本内容中有包含银行行业关键字，然后不符合我们想要的过滤条件的
    elif (
            (
                re.search(r'银行|农[信|金|行]|信合|支行|银联|信用社|商行|广发',
                          app_name) is not None
                or re.search(r'银行|农[信|金|行]|信合|支行|银联|信用社|商行|广发', suspected_app_name) is not None
            )
            or
            (
                re.search(
                    r'银行|农[信|金|行]|信合|支行|银联|信用社|商行|信用卡|广发|人民币|财付通|快捷支付|转账|汇款|存款|取现|消费|微信红包|支出|存入|我行|利息|本金|活期', msg) is not None
                and
                (
                    re.search(r'未识别', app_name) is not None
                    and re.search(r'未识别', suspected_app_name) is not None
                )
            )
        ) and \
            (re.search(r'物流|取件|车次|您预订的|您购买的|派出所|早盘提示|证券|纸黄金|股票|债券|信托|操手|涨跌|基金|申购|赎回', msg) is None):
        return True
    else:
        return False
