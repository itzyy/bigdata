#!/usr/bin

#引入环境变量
source /etc/profile



i=0;j=1

while (($j>=$i))
 do
     echo $j
     yesterday = `date -d "1 day ago" +"%Y-%m-%d"`

     let j =j-1

     cd /opt/work

     output = /user/hive/warehouse/fruit.db/origin_access_stat/%{yesterday}
     input = /test/access.log.2015-12-30
     su hdfs hadoop jar ./jar/hadoop-1.0-SNAPSHOT-jar-with-dependencies.jar cn.kepuchina.mr.AccessLogCleaner ${input} ${output}
done



