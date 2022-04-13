# 写这个脚本是为了解析从认知平台上下载的数据
# 脚本的输入是认知平台下载的数据
# 脚本的输出是支持训练的数据格式

# preprocess.ds_txt_final
# preprocess.ds_txt_final_sample
# nlp_dev.template_final

# 基础数据解析脚本
with open("car_sample.txt", "r", encoding="utf-8") as f, open("new_car_sample.txt", "a+", encoding="utf-8") as f1:
    data_lst = f.readlines()
    for lines in data_lst[1:]:
        line = lines.strip().split("\\x01")
        text = line[0]
        label = line[1]
        new_line = text + "\t" + label + "\n"
        f1.write(new_line)