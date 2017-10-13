package cn.spark.studt.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.awt.*;
import java.util.*;
import java.util.List;

/**
 * 使用window窗口函数
 * 案例：
 *      热点搜索词滑动统计，每隔10秒钟，统计最近60秒中的搜索词的搜索频次，并打印出排名最靠钱的3个搜索词以及出现次数
 *  场景：
 *       如我们关心过去30分钟大家正在热搜什么，并且每5分钟更新一次，这就使得热点内容是动态更新的，当然更有价值。
 */
public class WindowHostWord {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(WindowHostWord.class.getSimpleName())
                .setMaster("local[2]");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));

        JavaReceiverInputDStream<String> socketinfos = jsc.socketTextStream("hadoop7", 9999);

        //在输入信息中查找关键词
        JavaPairDStream<String, Integer> keywordRDD = socketinfos.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                //返回关键词
                return s.split(" ")[1];
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            //对搜索词加上初始次数
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        //使用window函数对一段时间内的rdd进行处理
        //第二个参数:是窗口的长度,即对最近多长的数据进行收集
        //第三个参数:是间隔时间,即每隔多长时间启动数据收集
        //截至到searchWordCountSDStream是不会进行计算的,等待滑动时间到了以后,10秒钟,就会将之前的60秒的RDD,聚合起来,然后统一执行reduceByKey
        //所以这里的 reduceByKeyAndWindow是针对每个窗口执行计算的,而不是针对某个DStream中的RDD
        JavaPairDStream<String, Integer> keyword60sRDD = keywordRDD.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        },Durations.seconds(60),Durations.seconds(10));

        //到这里为止,就已经可以做到,每隔10秒钟,出来,之前60秒的收集到的单词的统计次数
        //执行transform操作,因为一个窗口,就是一个60秒钟的数据,会变成一个RDD,然后,对这个RDD
        //根据每个搜索词出现的频率进行排序,然后获取排行前三的热点搜索词
        JavaPairDStream<String, Integer> searchTop3KeyCountDStream = keyword60sRDD.transformToPair(new Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>>() {
            @Override
            public JavaPairRDD<String, Integer> call(JavaPairRDD<String, Integer> s) throws Exception {
                //反转数据格式为<count,keyword>
                JavaPairRDD<Integer, String> countKeywordRDD = s.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                    @Override
                    public Tuple2<Integer, String> call(Tuple2<String, Integer> s1) throws Exception {
                        return new Tuple2<Integer, String>(s1._2, s1._1);
                    }
                });

                JavaPairRDD<Integer, String> sortCountKeywordRDD = countKeywordRDD.sortByKey(false);
                JavaPairRDD<String, Integer> keywordCountRDD = sortCountKeywordRDD.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<Integer, String> s) throws Exception {
                        return new Tuple2<String, Integer>(s._2, s._1);
                    }
                });

                List<Tuple2<String, Integer>> takeTop3KeywordRDD = keywordCountRDD.take(3);
                for (Tuple2<String, Integer> keywordCount :
                        takeTop3KeywordRDD) {
                    System.out.println(keywordCount._1 + "============" + keywordCount._2);

                }
                return s;
            }
        });
        //只是为了触发job的执行，所有必须要有output操作。
        //异常:No output operations registered, so nothing to execute
        searchTop3KeyCountDStream.print();
        //启动流的执行
        jsc.start();
        jsc.awaitTermination();
        jsc.close();


    }

}
