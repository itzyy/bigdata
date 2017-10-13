#!/usr/bin

#初始化配置文件
source /etc/profile

#设置时间
state_date=date -d '1 day ago' +"%Y-%m-%d"

#开始执行

echo "job start"

echo `date +'%Y-%m-%d'`
cd /opt/work

echo "AccessLogCleaner.sh"

# 执行数据清洗脚本
sh /opt/work/shell/accessCleaner.sh > /opt/work/log/accessCleaner.log.${state_date} 2>&1

####执行数据分析脚本

echo "job end"

