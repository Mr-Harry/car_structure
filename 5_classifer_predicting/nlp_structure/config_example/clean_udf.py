import re


def _format(text, format_func):
    entities = _basic_clean(text)
    if entities:
        entities = [format_func(entity) for entity in entities]
        entities = [entity for entity in set(
            entities) if not _is_empty_str(entity)]
        if entities:
            return SEP.join(entities)


def _basic_clean(text, least_length=1):
    """基本清洗

    把算法输出的分隔符去掉，实体的索引去掉，单字段多实体的情况，以空格相连，
    return list
    空值返回None
    """
    if _is_empty_str(text):
        return None
    words = text.split('#ALGO_ITEM_SEP#')                           # 去掉多实体分隔符
    words = [re.sub(r"[0-9]+\$@\$", "", word) for word in words]    # 去掉实体索引
    words = [word.strip() for word in words]                        # 去除空格
    words = [word for word in words if len(word) >= least_length]   # 至少长度为2
    words = list(set(words))
    if len(words) == 0:
        return None
    return words


def base_udf(text):
    entities = _basic_clean(text)
    if entities:
        entities = [entity for entity in list(
            set(entities)) if not _is_empty_str(entity)]
        if entities:
            return SEP.join(entities)


# 多实体字段的连接符
SEP = '#ALGO_ITEM_SEP#'


def _is_empty_str(text):
    if not text or re.search(r'^\s+$', text):
        return True
    return False


def amount(text):
    text = re.sub('^[^-\d\.]+|[^-\d\.万千百]+$', '', text)
    text = re.sub(',|，| ', '', text)
    if re.search('千万', text):
        tag = 10000000
    elif re.search('百万', text):
        tag = 1000000
    elif re.search('十万', text):
        tag = 100000
    elif re.search('万', text):
        tag = 10000
    elif re.search('千', text):
        tag = 1000
    elif re.search('百', text):
        tag = 100
    else:
        tag = 1
    text = re.sub('^[^-\d\.]+|[^-\d\.]+$', '', text)

    if re.match('-?\d+\.\d{0,2}$', text) or re.match('-?\d+$', text):
        return '{:.2f}'.format(float(text) * tag)
    else:
        return ''


def amount_udf(text):
    return _format(text, amount)


def loan_type(text):
    if re.search('车.{0,10}(贷|还|扣|分期|按揭|发放)', text):
        return '车贷'
    if re.search('(房|公积金|公寓)[一-龢a-e]{0,6}贷', text):
        return '房贷'
    if re.search('白条|消费贷|花呗|购物|租赁|租金|考拉海购|唯品会|蘑菇街|搬家|用途|租房|加油|按揭|整租|合租|账单|家装|家电|游学|高端游|邮轮|租赁|租车|住院|爸妈游|蜜月游|摄影|旅行|亲子游|跟团游|国外游|特价旅游|出境游|周边游|自助游|一日游|自由行|境内游|美容|整容|整形|购物|装修|装饰|花费|购买|教育|分期购|消费购|旅游|出国|签证|装饰|婚纱|结婚|婚庆|婚嫁|购买|医疗|生活开销|家电|购机|合约机|助学|分期消费|消费分期|牙齿|口腔|美白|祛斑|垫下巴|医疗美容|医美|美莱|隆鼻|垫下巴|双眼皮|近视|除皱|分期购|热玛吉|吸脂|塑形|抗衰|新氧|脂肪|开眼角|植发|黄金微针|水光针|自体脂肪|拉皮|[去祛].+?纹|假体|乳头|副乳|矫正|洗牙|半永久|溶脂|减脂|丰[卧唇太眉眉眼耳臀面额鼻]|水光针|小气泡美肤|皮秒激光|黄金微针|美肤|焕肤|微针美肤|广州紫馨|除皱瘦脸|养生馆|影城|影视|电影|娱乐|影票|门票|车票|高铁票|火车票|动车票|汽车票|轮船票|医院|门诊|住院|烫发|染发|住宿|宾馆|酒店|机票|航班', text):
        return '消费贷'
    return '现金贷'


def loan_type_udf(text):
    return _format(text, loan_type)


def clean_card_number(text):
    text = re.sub('^[^0-9]+|[^0-9]+$', '', text)
    if re.search('[^0-9]', text):
        return ''
    return text


def clean_card_number_udf(text):
    return _format(text, clean_card_number)


def income_transaction_type(text):
    if re.search('补贴|补助', text) and re.search('休假|加班|村干|村社干|网格|村医|驻村|公务员|公益性岗|村长|伙食|餐费|餐补|通讯|交通|差旅费|驻勤', text):
        return '工资'
    if re.search('工资|绩效|奖金|年终奖|考核|业绩|薪资|薪金|薪酬|薪卡', text):
        return '工资'
    if re.search('贷款', text):
        if re.search('转账存入|来账金额|收入|增加人民币|入账人民币|贷款发放存入|转存|入账|贷款放款|贷款发放|发放贷款|划款入账|账户转入|转存人民币|存入|转入|贷款金额|网上增加|收到贷款', text) \
                and re.search('请警惕贷款|对公|收回贷款|骗局|比上日|比年初|不得|支出|净收入|保证金|利息|账单金额|扣款|贷款股份有限公司您名|应还|还款|校验码|=|及时存入|到期|验证码|应该归还|及时|存入足额|还贷款|备足资金存入|归还|存款|贷款客户|还贷款|逾期|还款日', text) is not None:
            return '个人贷款收入'
        elif re.search('转账存入|来账金额|收入|增加人民币|入账人民币|贷款发放存入|转存|入账|贷款放款|贷款发放|发放贷款|划款入账|账户转入|转存人民币|存入|转入|贷款金额|网上增加|收到贷款', text) \
                and re.search('比上日|比年初|不得|支出|净收入|保证金|利息|账单金额|扣款|贷款股份有限公司您名|应还|还款|校验码|=|及时存入|到期|验证码|应该归还|及时|存入足额|还贷款|备足资金存入|归还|存款|贷款客户|还贷款|逾期|还款日', text) is not None:
            return '企业贷款收入'
        else:
            return ''
    return ''


def income_transaction_type_udf(text):
    return _format(text, income_transaction_type)


def outcome_transaction_type(text):
    if re.search('您单位尾号|贵司|您.{0,10}单位(账户|账号)|您的?.{0,10}公司账户|对公(账户|账号)|贵公司|贵单位|贵企业|尊敬的商户|您名下的商户|您(末四位为|尾号为).*的企业账户', text):
        return '企业'

    if re.search('还款|贷款|扣还|还款|偿还|转账还|还信用卡转出|办理|还信用卡|转出至信用卡|支付信用卡|转本行贷记卡', text) \
       and re.search('信用卡|贷记卡', text) and re.search('足够金额|签约|消费|ETC|足够余额', text) is not None:
        return '信用卡还款'

    if re.search('贵公司|贵司|您公司|贵单位|您单位|您的公司', text) and re.search('贷款', text) \
            and re.search('扣款|归还|自动扣息|贷款还本|贷款还息|贷款本息支出|贷款批量收款|还款支出|贷款还本息|贷款还款|扣划贷款本息|还本|还贷款|本息还款|贷款回收|回收贷款|贷款正常还款|还款取出|扣贷款|贷款扣划|贷款扣款|柜面还款|收付贷款本息|还逾期贷款|扣还', text) \
            and re.search('还款日|补足|应还|签约|余额不足|收回贷款|将于', text) is not None:
        return '企业贷款还款'

    if re.search('扣款|归还|自动扣息|贷款还本|贷款还息|贷款本息支出|贷款批量收款|还款支出|贷款还本息|贷款还款|扣划贷款本息|还本|还贷款|本息还款|贷款回收|回收贷款|贷款正常还款|还款取出|扣贷款|贷款扣划|贷款扣款|柜面还款|收付贷款本息|还逾期贷款|扣还', text) \
            and re.search('贷款', text) \
            and re.search('补足|应还|签约|余额不足|授权|将于|余额充足', text) is not None:
        return '个人贷款还款'

    # if re.search('还款|贷款|扣还', text) and re.search('贷款|将于.*扣还', text):
    #     return '贷款还款'
    return ''


def outcome_transaction_type_udf(text):
    return _format(text, outcome_transaction_type)


def amount_unit(text):
    if re.search('人民币|RMB|CNY|人民|民币|cny|￥', text):
        return '元'
    elif re.search('万元', text):
        return '万元'
    elif re.sub(',|，|。| |\.', '', text) == '万':
        return '万元'
    elif re.search('美元|USD|usd|\$', text):
        return '美元'
    elif re.search('欧元', text):
        return '欧元'
    elif re.search('加拿大元', text):
        return '加拿大元'
    elif re.search('英镑', text):
        return '英镑'
    elif re.search('万港币', text):
        return '万港币'
    elif re.search('法郎|FRF|frf', text):
        return '法郎'
    elif re.search('澳元', text):
        return '澳元'
    elif re.search('港币|港元', text):
        return '港币'
    elif re.search('澳大利亚元', text):
        return '澳大利亚元'
    elif re.search('日元', text):
        return '日元'
    elif re.search('韩元', text):
        return '韩元'
    elif re.search('元', text):
        return '元'
    else:
        return ''


def amount_unit_udf(text):
    return _format(text, amount_unit)


def prepayment(text):
    if re.search('提前还款', text):
        if re.search('预约', text):
            return '预约提前还款'
        if re.search('应还|对账单|还款日|过期|资金安全|到期|还款流程', text) is not None or re.search('手续费最低|无需支付违约金|若后续分期还款|还款日|或者提前还款|提前还款手续费收取标准|没有违约金|请您提前还款|建议您提前还款|随时提前还款|需要办理分期提前还款|不能撤销|如您需归还未到期|需提前还款|需要提前还款|若您为提前还款|营业时间|批量还款|到期日|如提前还款|方便|提前还款,手续费不予退还|若为提前还款|提前还款收剩余|提前还款收取剩余|若提前还款|提前还款请|如需提前|或已申请|如您需|如您要|会产生提前还款手续费|客户经理您好|支持提前还款|能否提前还款|后续分期提前还款|提前还款试算|选择提前还款|分期不能撤销|随时提前还款|任何违约金|未到账单日提前还款|优惠政策|e分期提前还款|可能希望', text) is not None or (re.search('提前还款日', text) and re.search('如需申请提前还款', text) is not None) or re.search('如需操作金条提前还款|需要提前还款|提前还款需同时满足|请确保还款日期提前还款|提前还款操作|帮我查一下|提前还款银行咨询|违约处置|具体细则|没有违约金|提前还款收取剩余|点击查看详情,提前还款|不收违约金|提前还款—办理|随时提前还款|提前还款:|提前还款G2|收取剩余本金|结案处理|涉及违约金|还款流程|可以提前还款|无法提前还款|含提前还款|点击提前还款|也可提前还款|提前还款会正常收取|融资方提前还款|参与提前还款|若操作了提前还款|提前还款等|提前还款请按规定|若已提前还款|提前还款可避免|提前还款试算|需提前还款|请提前还款|直接咨询|计算提前还款金额|如需提前还款', text):
            return '提前还款'


def prepayment_udf(text):
    return _format(text, prepayment)


def status(text):
    if re.search('失败|未通过|很遗憾|未成功', text):
        return '失败'
    if re.search('成功|已通过', text):
        return '成功'


def status_udf(text):
    return _format(text, status)


def null_value(text):
    return None


P_NAME = '(?:[王李张刘陈杨赵黄吴周孙徐朱马胡郭高何林罗郑宋唐谢梁韩许曹冯于田邓彭董肖袁曾潘杜蒋蔡魏程丁姜吕余沈任叶苏崔姚金范卢汪石贾谭陆夏付钟孟方邹白廖秦熊尹薛邱闫江侯顾段韦史雷郝龙邵陶钱贺龚毛万戴武严孔向汤常黎代葛康安邢洪施齐牛文温乔倪樊赖易聂莫章鲁覃殷庞耿颜岳翟申伍庄左焦俞柳欧兰关曲祝傅毕包冉季纪单尚梅舒甘苗谷霍盛童裴路成宁辛阮柯骆管詹靳涂鲍凌翁喻柴祁华游房牟解符滕蒲艾穆尤时屈饶司宫蓝蒙吉缪车候窦项阳褚费冷佟娄戚卓芦卜连景柏宗隋臧沙米边晏卞瞿席栾商古闵丛桂仇仲党桑刁应邬郎岑卫荣全迟简姬寇苟苑明虞甄鞠和巩师阎查敖池封佘占郁麻谈胥匡荆乐储冀巫麦惠南官班栗鄢楚燕奚农皮玉朴普满蔺闻伊谌楼盖丘阚巴花岩海索祖国原阿邝帅初禹屠强平廉井狄郜相粟权门支漆丰仝位邸云凡计厉卿宣戈薄修豆敬鹿邰浦芮都来赫母杭战晋宿伏亓练雍劳印裘阙晁公诸那况嵇盘逯达衣辜才居鲜羊冼茅勾戎隆於但危银慕亢干布信区由萧富湛束哈茹未扎腾展拉次檀尉阴昝智衡元荀贡容逄呼幸钮渠税矫乌郗鄂户揭字扈习蹇多藏操步冶昌青弓仁斯庹宾尧虎保东谯铁濮奉木侍靖旷巨尼竺刀么年种咸黑水历别雒訾寿山德从贝宝茆轩加菅泽折格利綦过旦宛泮经先蔚生隗暴琚邴玄冒蒯郄巢化回依续扬郇随买贲央英郦娜植果扶凤西俸汲伦旺顿滑要仓洛嘎眭卯籍宦畅延台茶糜汝双闭皇后寻宇招赛圣怀吝寸慈粱刚仵仉资庾锁员纳塔拜佐密临毋其戢望言库潭笪忻沃昂开撒广缑郏鱼薜朗崇萨禚俄波刑咪绳淡凃宓措拓益滚禄底竹仪励尕郅恽苍仰承同律红睢犹钦禤永神弋妥端沐贵蒿拥太线呙秘朝归俎念肇琼纵甯春法松颉亚可远秋火光及贠轻补自牙雪香力运摆土克所越静闪堵降思令卡淳淦焉问苌槐类图将慎虢喜千庆传热甲吾义杲接须宰桓揣完者弭喇闾伯彦丑色疏丹侬校乜牧更秀衷排院甫羿提教磨产朵主杞美镇子伞勇]|万俟|司马|上官|欧阳|夏侯|澹台|濮阳|淳于|巫马|百里|乐正|仲孙|单于|太叔|申屠|公孙|司徒|钟离|宇文|长孙|慕容|费莫|富察|第五|南宫|漆雕|公西|壤驷|司寇|子车|锺离|鲜于|段干|公良|尉迟|诸葛|闻人|东方|赫连|皇甫|轩辕|令狐|司空|碧鲁|纳喇|乌雅|范姜|谷糜|梁丘|南门|东门|宰父|端木|颛孙|三小|公羊|黄辰|左丘|呼延|西门|夹谷|谷梁|微生|羊舌|张廖)[一-龢]{1,2}'


def name_format(text):
    res = re.findall(P_NAME, text)
    if res:
        return res[0]


def name_clean(text):
    return _format(text, name_format)


def date_format(date_text):
    if date_text == '' or date_text is None:
        return None
    try:
        date_text_f = _date_format(date_text)
        return date_text_f
    except Exception as e:
        return None


def date_clean(text):
    return _format(text, date_format)


def _date_format(date_text):
    YEAR = 'XXXX'
    MONTH = 'XX'
    DAY = 'XX'
    MINUTE = 'XX'
    SECOND = 'XX'
    SEQ = ' '

    def date_format_(date):
        m = re.match(r'([\d|X]{2,4})-([\d|X]{1,2})-([\d|X]{1,2})', date)
        if m:
            year, month, day = m.group(1), m.group(2), m.group(3)
            if len(year) == 2 and year != 'XX':
                year = '20' + year
            if len(month) == 1 and month != 'X':
                month = '0' + month
            if len(day) == 1 and day != 'X':
                day = '0' + day
            date = '-'.join([year, month, day])
        else:
            date = None
        return date
    m = re.search(
        r'(\d{2,4})(?:年|/|-)(\d{1,2})(?:月|/|-)(\d{1,2})(?:日|)', date_text)
    if m:
        year, month, day = m.group(1), m.group(2), m.group(3)
        date_text = '-'.join([year, month, day])
        return date_format_(date_text)
    m = re.search(r'(\d{2,4})(?:年|/|-)(\d{1,2})(?:月|)', date_text)
    if m:
        year, month, day = m.group(1), m.group(2), DAY
        date_text = '-'.join([year, month, day])
        return date_format_(date_text)
    m = re.search(r'(\d{1,2})(?:月|/|-)(\d{1,2})(?:日|)', date_text)
    if m:
        year, month, day = YEAR, m.group(1), m.group(2)
        date_text = '-'.join([year, month, day])
        return date_format_(date_text)
    m = re.search(r'(\d{2,4})(?:年)', date_text)
    if m:
        year, month, day = m.group(1), MONTH, DAY
        date_text = '-'.join([year, month, day])
        return date_format_(date_text)
    m = re.search(r'(\d{1,2})(?:月)', date_text)
    if m:
        year, month, day = YEAR, m.group(1), DAY
        date_text = '-'.join([year, month, day])
        return date_format_(date_text)
    m = re.search(r'(\d{1,2})(?:日)', date_text)
    if m:
        year, month, day = YEAR, MONTH, m.group(1)
        date_text = '-'.join([year, month, day])
        return date_format_(date_text)
    return None


# 需要创建自定义UDF函数字典对象：clean_udf_dict，目前支持1，2，3个自定义参数输入。分别需要创建：
# key为：clean_udf_dict1，clean_udf_dict2，clean_udf_dict3 的三个udf配置字典对象，样例如下：
clean_udf_dict = {
    'clean_udf_dict1': {
        "name_udf": name_clean,  # 姓名
        "amount_udf": amount_udf,  # 金额
        "loan_type_udf": loan_type_udf,  # 贷款类型
        "clean_card_number_udf": clean_card_number_udf,  # 卡号
        "income_transaction_type_udf": income_transaction_type_udf,  # 收入类型
        "outcome_transaction_type_udf": outcome_transaction_type_udf,  # 支出类型
        "amount_unit_udf": amount_unit_udf,  # 货币单位
        "date_clean_udf": date_clean,  # 日期
        "base_udf": base_udf,
        "status_udf": status_udf,
        "prepayment_udf": prepayment_udf}
}


# if __name__ == '__main__':
#     pass
