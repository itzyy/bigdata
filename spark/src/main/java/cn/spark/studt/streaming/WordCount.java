package cn.spark.studt.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import javax.xml.datatype.Duration;
import java.util.Arrays;

/**
 * Created by Zouyy on 2017/9/20.
 */
public class WordCount {

    public static void main(String[] args) throws InterruptedException {
        //创建sparkcontext对象
        //但是这里有一点不同，我们要给他设置一个master属性，但是我们测试的时候使用local模式
        //local后面必须跟一个方括号，里面填写一个数字，数字代表了，我们用几个线程来执行我们的spark streaming 程序
        SparkConf conf = new SparkConf()
                .setAppName("WordCount")
                .setMaster("local[2]");


        //创建javastreamingcontext对象
        //该对象，就类似于spark core中的javasparkcontext，就类似于spark sql中的sqlcontext
        //该对象除了接收sparkconf对象之外，还需要接收一个 batch interval 参数，也就是说，每收集多长时间的数据，划分为一个batch,进行处理
        //设置为1秒
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));


        //首先，会创建DStream，代表了一个数据源（比如kafka、socket）来持续不断的时间数据流
        //调用JavaStreamingContext的socketTextStream()方法，可以创建一个数据源为socket网络端口的数据流
        //JavaReceiverInputStream，代表了一个输入的Stream
        //socketTextStream()方法接收两个参数，一个是监听哪个主机上的端口，第二个是监听哪个端口
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("hadoop7", 9999);


        //到这里为止，你可以理解为javareceiverinputdstream中的，每隔一秒，会有一个RDD,其中封装了这一秒发送过来的数据
        //RDD的元素类型为String,即一行一行的文本
        //所以，这里JavaReceiverInputStream的泛型类型<String>,其实就代表了底层的RDD的泛型类型
        //开始对接收的数据，执行计算，使用spark core提供的算子，执行应用到Stream中即可
        //在底层，实际上是会对Stream中的一个个的RDD，执行我们应用在Stream上的算子
        //产生新的RDD，会作为新Stream中的RDD
        JavaPairDStream<String, Integer> wordcounts = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        })
                //这个时候，每秒的数据，一行一行的文本，就会被拆分为多个单词，words Dstream中RDD的元素类型，即为一个一个单词
                .mapToPair(new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                })
                //用sprak streaming与spark core很想象
                //唯一不同的是spark core中的JavaRDD、JavaPairEDD ,都变成了JavaStream、JavaPairStream
                .reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return integer + integer2;
                    }
                });

        //到此为止，我们就实现了实时的wordcount程序了
        //每秒中发送到指定socket端口上的数据，都会被lines Dstream接收到
        //然后lines DStream会把每秒的数据，也就是一行一行的文本，诸如hell world，封装为一个RDD
        //然后呢，就会对每秒中对应的RDD，执行后续的系列的算子操作
        //比如，对line rdd执行flatmap 之后，得到一个words, RDD 作为words DStream中的一个RDD
        //以此类推，直到生成最后一个，wordcounts RDD，作为wordcounts DStream中的一个RDD
        //此时，就得到了，每秒钟发送过来的数据的单词统计
        //但是，一定要注意，spark streaming的计算模型，就决定了，我们必须自己来进行中间缓存的控制
        //比如写入redis缓存
        //它的计算模型跟storm是完全不同的，storm是自己编写的一个一个的程序，运行在节点上，相当于一个一个的对象，可以自己在对象中控制缓存
        //但是spark本身是一个函数式编程的计算模型，所以，比如 在words或者pairs DStream中，没法在实例变量进行缓存
        //此时就只能将最后计算出的wordcounts中的一个一个的RDD,写入外部的缓存，或者持久化DB
        //最后，每次计算完，都打印一次着一秒钟的单词计数情况
        //并休眠5秒钟，以便于我们测试和观察

        wordcounts.print();
        Thread.sleep(5000);
        //首先对javastreamingcontext进行一下后续处理
        //必须调用javastreamingcontext的start()方法，整个spark streaming application才会启动执行
        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }

}
