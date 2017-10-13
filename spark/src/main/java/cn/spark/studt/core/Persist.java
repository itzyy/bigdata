package cn.spark.studt.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *  见笔记:http://8996cf42.wiz03.com/share/s/29BIZ230XQ8P2wNgwD2antBe1Ss0r83oH4C12sEknZ1TUCJy
 *使用cache进行RDD持久化
 * cache和persist的区别在于,cache是persist的一种简化方式,cache的底层就是调用的persist的无参版本,同时就是嗲用perist(MEMORY_ONLY),将
 * 数据持久化在内存中,如果需要从内存中清除缓存,那么可以使用unpersist()方法.
 */
public class Persist {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(Persist.class.getSimpleName())
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        //使用cache进行持久化
        JavaRDD<String> javaRDD = sc.textFile("C:\\Users\\LENOVO\\Desktop\\spark.txt").cache();

        long beginTime = System.currentTimeMillis();

        long count = javaRDD.count();
        System.out.println(count);

        long endTime = System.currentTimeMillis();
        System.out.println("cost1 "+(endTime-beginTime)+" milliseconds");

        beginTime = System.currentTimeMillis();
        //根据时间判断是否使用缓存
        count = javaRDD.count();
        System.out.println(count);

        endTime = System.currentTimeMillis();
        System.out.println("cost2 "+(endTime-beginTime)+" milliseconds");

        sc.close();

    }


}
