wanted_label = ['购车意向_看车询价','购车意向_买车','购车意向_摇号竞价','购车意向_学车考试','购车意向_车牌号预选','有车行为_记分办证','有车行为_年检年审','有车行为_洗车']
with open('predict_result.txt', 'r', encoding='utf-8') as f1:
    data_lst = f1.readlines()
    for target_label in wanted_label:
        index = 0
        with open(target_label+'.txt','w') as f2:
            for lines in data_lst:
                line = lines.strip().split('\\x01')
                label = line[0]
                text = line[1]
                if label == target_label and index < 1000:
                    f2.write(text+'\n')
                    index =  index + 1
                if index >= 1000:
                    break
        if index != 1000:
            print(target_label,"类数据量不足1000。有且只有",index)