package cn.spark.studt.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Int;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 1.对文本文件内的每个单词都统计出现的个数
 * 2.按照每个单词出现次数的数量,降序排序
 */
public class SortWordCount {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(SortWordCount.class.getSimpleName())
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> javaRDD = sc.textFile("C:\\Users\\LENOVO\\Desktop\\spark.txt");

        //通过flatmap算子,将行拆分成列
        JavaRDD<String> flatMap = javaRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });
        //通过maptopair算子,将数据转换成<string,integer>
        JavaPairRDD<String, Integer> mapToPair = flatMap.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        //通过reduceByKey根据相同key,进行汇总
        JavaPairRDD<String, Integer> reduceByKey = mapToPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        //通过maptopair重新定义输出的key,,value为下面的排序做准备
        JavaPairRDD<Integer, String> countWords = reduceByKey.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> s) throws Exception {
                return new Tuple2<Integer, String>(s._2, s._1);
            }
        });
        //通过sortByKey进行升序排序
        JavaPairRDD<Integer, String> sortByKey = countWords.sortByKey(false);

        JavaPairRDD<String, Integer> rdd = sortByKey.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> s) throws Exception {
                return new Tuple2<String, Integer>(s._2, s._1);
            }
        });
        //通过foreach进行打印
        rdd.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> s) throws Exception {
                System.out.println(s._1+"======="+s._2);
            }
        });


    }

}
