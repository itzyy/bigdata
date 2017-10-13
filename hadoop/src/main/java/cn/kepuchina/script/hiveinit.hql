create database if not exists fruit comment '统计库';

use analysis;

--创建原始数据表
create table if not exists k_origin_access_stat_tbl(
appid	string	comment 'appid',
   macip	string	comment 'macip',
   userip	string	comment '用户id',
   loginType	string	comment '登陆类型',
   status	string	comment '状态',
   httpReferer	string	comment '网站入口',
   accesstime	string	comment '访问时间'
)comment	'原始数据表'
partitioned by(stat_time string)
row format delimited
fields terminated by '\t'
stored as textfile;

--登陆用户Uv、Pv
create table if not exists k_login_access_stat_tbl(
    appid string comment 'appid',
    uv string comment 'appid维度的uv量',
    pv string comment 'appid维度的pv量',
    genTime string comment '生成时间'
) comment 'appid维度的Uv和Pv数据表'
partitioned by (stat_time string)
row format delimited
fields terminated by '\t'
stored as textfile;

--APP维度的Uv、Pv
create table if not exists k_app_access_stat_tbl(
     appid string comment 'appid',
     uv string comment 'appid维度的uv量',
     pv string comment 'appid维度的pv量',
     genTime string comment '生成时间'
 ) comment 'appid维度的Uv和Pv数据表'
partitioned by (stat_time string)
row format delimited
fields terminated by '\t'
stored as textfile;

--次日留存
create table if not exists k_one_remain_stat_tbl(
     appid string comment 'appid',
     uv string comment 'appid维度的uv量',
     genTime string comment '生成时间'
 ) comment 'appid维度的Uv和Pv数据表'
partitioned by (stat_time string)
row format delimited
fields terminated by '\t'
stored as textfile;

--次日留存
create table if not exists k_seven_remain_stat_tbl(
     appid string comment 'appid',
     uv string comment 'appid维度的uv量',
     genTime string comment '生成时间'
 ) comment 'appid维度的Uv和Pv数据表'
partitioned by (stat_time string)
row format delimited
fields terminated by '\t'
stored as textfile;
