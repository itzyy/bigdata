package cn.spark.studt.streaming;

import cn.spark.studt.core.TranformationOperation;
import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * 基于tranform的实时广告计费日志黑名单过滤
 * 通过transform可以将每个batch，与一个特定的RDD进行jion的操作，
 */
public class TransformBlacklist {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(TranformationOperation.class.getSimpleName())
                .setMaster("local[2]");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));

        JavaReceiverInputDStream<String> socketDatas = jsc.socketTextStream("hadoop7",9999);

        //构造黑名单
        List<Tuple2<String,Boolean>> blocklist =new ArrayList<Tuple2<String,Boolean>>();
        blocklist.add(new Tuple2("tom", true));

        //第一步：创建黑名单RDD对象
        final JavaPairRDD<String, Boolean> blockInfoRDD = jsc.sc().parallelizePairs(blocklist);

        //第二步：创建<username,date usernaem>数据格式
        final JavaPairDStream<String, String> requestInfoRDD = socketDatas.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<String, String>(s.split(" ")[1], s);
            }
        });

        //第三步：request信息关联黑名单，过滤掉黑名单，生成结果为<username>
        requestInfoRDD.transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaPairRDD<String, String> userClickInfosRDD) throws Exception {
                //访问信息关联黑名单
                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> joinRDD = userClickInfosRDD.leftOuterJoin(blockInfoRDD);

                return joinRDD.filter(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> s) throws Exception {
                        Optional<Boolean> booleanOptional = s._2._2();
                        //这里的tuple,就是每个用户，对应的访问日志，和在黑名单中的状态
                        if (booleanOptional.isPresent() && s._2._2.get()) {
                            return false;
                        }
                        return true;
                    }
                }).map(new Function<Tuple2<String,Tuple2<String,Optional<Boolean>>>, String>() {
                    @Override
                    public String call(Tuple2<String, Tuple2<String, Optional<Boolean>>> s) throws Exception {
                        System.out.println("================"+s._1);
                        return s._2._1;
                    }
                });
            }
        }).print();

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }

}
