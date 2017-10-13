package cn.spark.studt.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class KafkaReceiverWordCount{

    public static void main(String[] args) {
        //创建sparkcontext，并行度为2
        SparkConf conf = new SparkConf()
                .setAppName(HDFSWordCount.class.getSimpleName())
                .setMaster("local[2]");


        //创建javaStreaming对象，每5秒获取一个batch数据
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));

        Map<String, Integer> topicThreadMap = new HashMap<String, Integer>();
        //topic为wordcount,一个partition
        topicThreadMap.put("WordCount",1);
        JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(
                jsc,
                "hadoop7:2181",
                "DefaultConsumerGroup",
                topicThreadMap
        );

        lines.print();
        //编写算子
        JavaPairDStream<String, Integer> wordcounts = lines.flatMap(new FlatMapFunction<Tuple2<String,String>, String>() {
            @Override
            public Iterable<String> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                System.out.println("=================="+ stringStringTuple2._1);
                return Arrays.asList(stringStringTuple2._2.split("\t"));
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
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
