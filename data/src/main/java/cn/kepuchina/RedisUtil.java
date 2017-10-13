package cn.kepuchina;

import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by LENOVO on 2017/7/25.
 */
public class RedisUtil {

    private static JedisPool jedisPool=null;
    private static Logger logger = Logger.getLogger(RedisUtil.class);


    //创建链接
    public static synchronized Jedis getJedis(){
        try {
            if(jedisPool==null){
                JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
                //设置jedis中最大连接数
                jedisPoolConfig.setMaxTotal(100);
                //设置jedis中最大空闲连接数
                jedisPoolConfig.setMaxIdle(10);
                //设置jedisz中超时限制
                jedisPoolConfig.setMaxWaitMillis(10000);
                //设置是否测试链接的可用性
                jedisPoolConfig.setTestOnBorrow(true);
                jedisPool = new JedisPool(jedisPoolConfig, "192.168.239.12", 6379);
            }
        } catch (Exception e){
            logger.error(e.getMessage());
        }
        return jedisPool.getResource();
    }

    //关闭链接
    public static void closeJedis(Jedis jedis){
        if(jedis!=null){
            jedisPool.close();
        }
    }
}
