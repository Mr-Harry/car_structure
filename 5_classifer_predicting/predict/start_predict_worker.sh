
function help {
   echo "使用帮助："
   echo "-s 数据下发端口号"
   echo "-d 结束通知端口号"
   echo "-u 同步退出端口号"
   echo "-g 使用的GPU序号，格式为 \"1 2 3\" 表示启动三个Work进程，分别使用1 2 3号GPU,必须参数"
   echo "-m 模型存放目录,必须参数"
   echo "-b batch_size大小,默认2048"
   echo "-l 日志输出目录"
   exit 1
}

function print_value {
   echo "-s=$host_ip_port"
   echo "-d=$dish_ip_port"
   echo "-b=$batch_size"
   echo "-g=\"$gpus\""
   echo "-m=$root_path"
}

batch_size=1024
gpus=''
root_path=''
log_path='./'

while getopts "s:d:g:m:b:l:h" arg
do
        case $arg in
             s)
             host_ip_port=$OPTARG
             ;;
             d)
             dish_ip_port=$OPTARG
             ;;
             g)
             gpus=$OPTARG
             ;;
             m)
             root_path=$OPTARG
             ;;
             b)
             batch_size=$OPTARG
             ;;
             l)
             log_path=$OPTARG
             ;;
             h)
             help
             ;;
             ?)
             help
             ;;
        esac
done

if [ "$gpus" == '' ] ; then
   echo 'GPU使用列表 未赋值,使用-g指定GPU使用列表'
   help
elif [ "$root_path" == '' ] ; then
   echo '模型存放路径 未赋值,使用-m指定模型存放目录'
   help
fi

for i in $gpus
do
nohup python predict_worker.py --batch_size $batch_size --root_path $root_path --host_ip tcp://127.0.0.1:$host_ip_port --dish_ip tcp://127.0.0.1:$dish_ip_port --gpu $i  --log_file $log_path/w_$RANDOM.log > /dev/null &
done