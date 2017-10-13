CREATE DATABASE IF NOT EXISTS analysis;

USE analysis;
/**
*	登录维度Uv和Pv
*/
CREATE TABLE IF NOT EXISTS k_login_access_stat_tbl(
    `appid` VARCHAR(4) COMMENT 'appid',
    `uv` INT COMMENT 'appid维度的uv量',
    `pv` INT COMMENT 'appid维度的pv量',
    `stat_time` DATE COMMENT '生成时间'
) COMMENT 'appid维度的Uv和Pv数据表';

CREATE TABLE IF NOT EXISTS k_app_access_stat_tbl(
    `appid` VARCHAR(5) COMMENT 'appid',
    `uv` INT COMMENT 'appid维度的uv量',
    `pv` INT COMMENT 'appid维度的pv量',
    `stat_time` DATE COMMENT '生成时间'
) COMMENT 'appid维度的Uv和Pv数据表';



/**
*次日留存
*/
create table if not exists k_one_remain_stat_tbl(
     appid VARCHAR(4) comment 'appid',
     uv INT comment 'appid维度的uv量',
     genTime date comment '生成时间'
 ) comment 'appid维度的Uv和Pv数据表';

/**
*--次日留存
*/
create table if not exists k_seven_remain_stat_tbl(
     appid VARCHAR(4) comment 'appid',
     uv INT comment 'appid维度的uv量',
     genTime date comment '生成时间'
 ) comment 'appid维度的Uv和Pv数据表';
