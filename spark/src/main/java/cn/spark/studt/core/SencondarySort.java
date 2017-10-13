package cn.spark.studt.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Int;
import scala.Tuple2;

/**
 * 自定义二次排序
 * 1.实现自定义的key排序,要实现Ordered接口和Serializable接口,在key中实现自己对多个列的排序算法
 * 2.将包含文本的RDD,映射成key自定义key,value为文本的JavapairRdd
 * 3.使用sortbykey算子按照自定义的key进行排序
 * 4.再次映射,提出自定义的key,只保留文本行
 */
public class SencondarySort {


    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(SencondarySort.class.getSimpleName())
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> javaRDD = sc.textFile("C:\\Users\\LENOVO\\Desktop\\sort.txt");


        JavaPairRDD<SecondarySortKey, String> pairRDD = javaRDD.mapToPair(new PairFunction<String, SecondarySortKey, String>() {
            @Override
            public Tuple2<SecondarySortKey, String> call(String s) throws Exception {
                String[] split = s.split(" ");
                SecondarySortKey sortKey = new SecondarySortKey(
                        Integer.valueOf(split[0]),
                        Integer.valueOf(split[1])
                );
                return new Tuple2<SecondarySortKey, String>(sortKey, s);
            }
        });

        JavaPairRDD<SecondarySortKey, String> sortByKey = pairRDD.sortByKey();

        JavaRDD<String> rdd = sortByKey.map(new Function<Tuple2<SecondarySortKey, String>, String>() {
            @Override
            public String call(Tuple2<SecondarySortKey, String> s) throws Exception {
                return s._2;
            }
        });

        rdd.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        sc.close();


    }

}
