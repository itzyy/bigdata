package cn.spark.studt.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

/**
* 广播变量
 * 使用广播变量,将数字乘以三
 */
public class BroadcastVariable {


    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("BroadcastVariable")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8);

        JavaRDD<Integer> javaRDD = sc.parallelize(numbers);

        final int factor =3;
        final Broadcast<Integer> broadcast = sc.broadcast(factor);

        JavaRDD<Integer> rdd = javaRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v) throws Exception {
                //读取broadcast的值
                return v * broadcast.value();
            }
        });

        rdd.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });


        sc.close();


    }

}
