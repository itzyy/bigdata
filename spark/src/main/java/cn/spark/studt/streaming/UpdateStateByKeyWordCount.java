package cn.spark.studt.streaming;

import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.net.Inet4Address;
import java.util.Arrays;
import java.util.List;

/**
 * 基于updateSteteByKey 算子实现缓存机制的实时wordcount程序
 * 实现汇总全局单词的数量
 */
public class UpdateStateByKeyWordCount {


    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(UpdateStateByKeyWordCount.class.getSimpleName())
                .setMaster("local[2]");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));

        //第一点：如果要使用updateStateByKey算子，必须设置一个checkpoint目录，开启checkpoint机制
        //这样的话才能把每个key对应的state除了在内存中有，那么是不是也要checkpoint一份
        //因为你要长期保存一份key的state的话，那么spark streaming是要求必须用checkpoint，以便于在
        //内存数据丢失的时候，可以从checkpoint中恢复数据

        //开启checkpoint机制，很简单，调用jsc的checkpoint() 方法，设置一个hdfs目录即可。
        jsc.checkpoint("hdfs://hadoop7:9000/spark/date/checkpint");

        JavaReceiverInputDStream<String> scoketinfos = jsc.socketTextStream("hadoop7", 9999);

        JavaPairDStream<String, Integer> onewords = scoketinfos.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });



        //之前使用reduceByKey只能对一个batch的单词进行计数
        //但是需要对全局的单词进行计数的话，可能通过redis，mysql等进行操作
        //但是，我们可以通过updateStateByKey，就可以实现直接通过spark维护一份每个单词的全局的统计次数

        //Optional 代表一个值的存在状态，可能存在，也可能不存在;如果存在则包含key对应的值
        JavaPairDStream<String, Integer> wordcounts = onewords.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

            //对于每个单词，每次batch计算的时候，都会调用这个函数
            //第一个参数：比如说一个hello，可能有2个1，(hello, 1) (hello, 1)，那么传入的是(1,1)，好像有点group然后reduce的感觉
            //第二个参数：值的是这个key之前的状态,state,其中泛型的类型是你自己指定的
            @Override
            public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
                Integer newValue = 0;

                if (state.isPresent()) {
                    newValue = state.get();
                }

                for (Integer i : values) {
                    newValue += i;
                }
                //创建一个Optional
                return Optional.of(newValue);
            }
        });

        wordcounts.print();
        jsc.start();
        jsc.awaitTermination();
        jsc.close();

    }


}
