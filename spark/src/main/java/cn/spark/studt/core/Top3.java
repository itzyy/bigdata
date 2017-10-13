package cn.spark.studt.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

/**
* 对文本文件中的数字,最大的前3个
 */
public class Top3 {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName(Top3.class.getSimpleName())
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> javaRDD = sc.textFile("C:\\Users\\LENOVO\\Desktop\\top3.txt");

        JavaPairRDD<Integer, String> pairRDD = javaRDD.mapToPair(new PairFunction<String, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(String s) throws Exception {
                return new Tuple2<Integer, String>(Integer.valueOf(s), s);
            }
        });

        JavaPairRDD<Integer, String> sortByKey = pairRDD.sortByKey(false);
        JavaRDD<String> mapRdd = sortByKey.map(new Function<Tuple2<Integer, String>, String>() {
            @Override
            public String call(Tuple2<Integer, String> v) throws Exception {
                return v._2;
            }
        });

        List<String> takes = mapRdd.take(3);
        for (String take:takes){
            System.out.println(take);
        }


    }

}
