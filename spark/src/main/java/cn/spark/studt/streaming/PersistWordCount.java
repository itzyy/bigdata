package cn.spark.studt.streaming;

import cn.spark.studt.streaming.util.ConnectionUtil;
import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 基于持久化机制的实时wordcount程序
 * 注意
 * 1.在RDD的foreach操作外部,创建connection;这样会导致connection对象序列化后传输到每隔taak中,而这种connection对象;
 * 实际上一般是不支持序列化的,也就无法传输
 * 2.在RDD的foreacha操作内部,创建connection,这种效率低下,因为他会导致对与RDD的每一条数据,都创建一个Connection对象.而通常来说,
 * connection的创建,是很消耗性能的
 * 合理的方式:
 * 1.使用RDD的foreachPartition操作,并且在该操作内部,创建connection对象,这样就相当于为RDD的每个partition创建一个connection对象
 * 2.自己手动封装一个静态连接池,使用RDD的foreachPaittion操作,并且在该操作内部,从静态连接池中,通过静态方法,获取到一个连接,使用
 * 之后在还回去,这样的话,甚至在多个RDD的partition之间,也可以复用连接了,而且可以让连接池采取懒创建的策略,并且空闲一段时间后,将其
 * 释放掉.
 */
public class PersistWordCount {



    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(UpdateStateByKeyWordCount.class.getSimpleName())
                .setMaster("local[2]");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));

        JavaReceiverInputDStream<String> scoketinfos = jsc.socketTextStream("hadoop7", 9999);
        jsc.checkpoint("hdfs://hadoop7:9000/spark/data/checkpint");
        JavaPairDStream<String, Integer> keywordCountRDD = scoketinfos.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        }).updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            @Override
            //第一个参数：比如说一个hello，可能有2个1，(hello, 1) (hello, 1)，那么传入的是(1,1)，好像有点group然后reduce的感觉
            //第二个参数：包含key之前的状态和之前的值
            public Optional<Integer> call(List<Integer> integers, Optional<Integer> optional) throws Exception {
                Integer newValue = 0;
                if (optional.isPresent()) {
                    newValue += optional.get();
                }
                for (Integer count :
                        integers) {
                    newValue += count;
                }
                //创建一个Optional对象
                return Optional.of(newValue);
            }
        });

        keywordCountRDD.print();
        //每次得到当前所有单词的统计次数之后,将其写入到存储mysql中,进行持久化,以便于后续的J2EE应用程序
        //进行输出显示
        keywordCountRDD.foreachRDD(new Function<JavaPairRDD<String, Integer>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, Integer> ss) throws Exception {

                ss.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Integer>> wordcounts) throws Exception {
                        Tuple2<String, Integer> wordcount = null;
                        Connection conn = ConnectionUtil.getConnection();
                        while (wordcounts.hasNext()) {
                            wordcount = wordcounts.next();
                            Statement stat = conn.createStatement();
                            String sql = "insert into wordcount values ('" + wordcount._1 + "',"+wordcount._2+")";
                            System.out.println("=================="+sql);
                            stat.executeUpdate(sql);
                        }
                        ConnectionUtil.returnConnection(conn);
                    }
                });
                return null;
            }
        });
        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }

}
