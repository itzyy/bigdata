package cn.spark.studt.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 *
 */
public class HDFSWordCount {

    public static void main(String[] args) {
        //创建sparkcontext，并行度为2
        SparkConf conf = new SparkConf()
                .setAppName(HDFSWordCount.class.getSimpleName())
                .setMaster("local[2]");

        //创建javaStreaming对象，每5秒获取一个batch数据
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));

        //监控hdfs目录，只要其中有新文件出现，就实时处理，获取hdfs数据，基于文件的数据源是没有receiver的，因此不会占用一个cpu core,处理完成后，
        // 即使文件的内容发生变化，也不会在继续处理了。
        JavaDStream<String> lines = jsc.textFileStream("hdfs://hadoop7:9000/spark/data/streaming");

        lines.print();
        //编写算子
        JavaPairDStream<String, Integer> wordcounts = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split("\t"));
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        //打印结果
        wordcounts.print();

        //开始执行
        jsc.start();
        //等待任务结束
        jsc.awaitTermination();
        //关闭访问
        jsc.close();


    }

}


