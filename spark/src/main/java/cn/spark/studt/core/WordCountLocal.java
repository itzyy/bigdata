package cn.spark.studt.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.codehaus.groovy.runtime.powerassert.SourceText;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 本地测试的wordcount程序
 * 如果需要在spark集群上运行，需要修改的，只有两个地方
 * 1、去除setMaster()方法
 * 2、我们针对的不是本地文件了，修改为hadoop hdfs 上的真正的存储大数据的文件
 * Created by Zouyy on 2017/9/4.
 */
public class WordCountLocal {

    public static void main(String[] args) {
        //编写spark应用程序
        //第一步：创建sparkconf对象，设置spark应用的配置信息
        SparkConf sparkConf = new SparkConf()
                .setAppName(WordCountLocal.class.getName())
                //设置spark应用程序要连接的spark集群的master节点的url
                .setMaster("local");
        //第二步：创建JavaSparkContext对象（所有spark功能的入口）
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        //第三部：要针对输入源，创建一个初始的RDD
        //输入源中的数据会打散，分配到RDD的每个partition中，从而形成一个初始的分布式的数据集
        //textFile:用于根据文件类型的输入源创建RDD的方法
        final JavaRDD<String> lines = sc.textFile("C:\\Users\\LENOVO\\Desktop\\spark.txt");
//                final JavaRDD<String> lines = sc.textFile("hdfs:/spark1:9000/spark.txt");

        //第四步：对初始RDD进行transformation操作，也就是一些计算操作
            //先将每一个拆分成单个单词
            //通常操作会通过创建function,并配合rdd的map、flatmap等算子来执行
            //funcation，通常，如果比较简单，则创建指定funcation的匿名内部类
            //flatmap算法：将RDD的一个元素，给拆分成一个或多个元素
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));//转成list
            }
        });

        //第五步：将每个单词，映射为（单词，1）的这种格式
            //maptopair:将每个元素映射为一个（v1,v2）这样的tuple2 类型的元素
            //第一个泛型元素：输入类型，第二个和第三个：输出的tuple2的第一个和第二个值
        JavaPairRDD<String, Integer> pairs= words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        });

        /**
         * 第六步：使用单词的key,统计每个单词出现的次数
         * reducekey：对每个key对应的reduce，都进行reduce操作，比如pair中分别有几个元素：分别为（hello,1），（hello,1），（hello,1）
         * reduce之后的结果，相当于每个单词出现的次数
         */
        JavaPairRDD<String, Integer> wordcounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        //第七步：触发程序的执行
        /**
         * 之前我们使用的flatMap，maptopair，reduceBykey这种操作，都叫做transformation操作
         * 使用action，比如，foreach来触发程序的执行
         */
        wordcounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> wordcount) throws Exception {
                System.out.println(wordcount._1+"\t"+wordcount._2);
            }
        });

        //关闭javasparkcontext
        sc.close();
    }

}
