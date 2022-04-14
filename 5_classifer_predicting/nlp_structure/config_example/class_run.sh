function get_list {
    gpu_num=$1
    work_num=$2
    s=""
    i=0
    while(( $i<$gpu_num ))
    do
        ii=0
        while(( $ii<$work_num))
        do
            s=$s" "$i
           let "ii++"
        done
       let "i++"
    done
    echo $s
}

source /etc/profile
mc cat minio/code/conf.tgz >> conf.tgz
tar xvf conf.tgz && rm conf.tgz
cp conf/* /etc/
cp conf/* /opt/

mc cat minio/code/k8s-0.0.1-SNAPSHOT.jar >> server-0.0.1-SNAPSHOT.jar
nohup java -jar server-0.0.1-SNAPSHOT.jar > log.log &
sleep 5

DIR="/root"
WORK_NUM=3  # 每个GPU可以开启的工作进程数，主要为了提高GPU使用效率。
echo $DIR
echo "拉取代码"  # 这些是结构化生产所必须的代码组件。
mc cat minio/code/rust_server2.tgz >> rust_server.tgz
mc cat minio/code/predict.tgz >> predict.tgz

echo "拉取模型文件" # 这里是需要准备打包上传的模型文件
mc cat minio/model/bankV1ClassModel0901.tgz >> bankV1ClassModel0901.tgz
echo "解压"
tar xvf rust_server.tgz && rm rust_server.tgz
tar xvf predict.tgz && rm predict.tgz
tar xvf bankV1ClassModel0901.tgz && rm bankV1ClassModel0901.tgz
mv bankV1ClassModel0901 predict/  # 对于分类任务，需要将模型文件夹移动到predict文件夹下
echo "启动"
gpu_list=`get_list GPU_NUM WORK_NUM`
# 启动模型Work
# ner任务:
# cd $DIR/predict && bash start_predict_worker.sh -s 9001 -d 9003 -g "$gpu_list" -l ./ -m $DIR/bank_ner/saved_model/  # -m指定模型目录
# 分类任务
cd $DIR/predict && bash start_predict_worker_classfiy.sh -s 9001 -d 9003 -n 9002 -g "$gpu_list" -l ./
# start_one.sh脚本提供数据分隔符自定义，需要预测的字段序号自定义，分别通过-p和-i指定，默认分隔符为'\t'，默认字段序号为4。
cd $DIR/rust_server && bash start_one.sh -s 9001 -n 9002 -d 9003 -i 5 -f $DIR/config.json -r 127.0.0.1:6663 -t 127.0.0.1:6663
# 判断最后执行是成功还是失败
if [ $? == 0 ]; then
    echo "完成"
    exit 0
else
    echo "失败"
    exit 1
fi
