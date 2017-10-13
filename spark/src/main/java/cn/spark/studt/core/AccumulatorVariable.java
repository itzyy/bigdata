package cn.spark.studt.core;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * Created by Zouyy on 2017/9/8.
 */
public class AccumulatorVariable {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("BroadcastVariable")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8);


        //调用accumnlator,声明accumnlator变量
        final Accumulator<Integer> accumulator = sc.accumulator(0);

        JavaRDD<Integer> javaRDD = sc.parallelize(numbers);

        javaRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer t) throws Exception {
                accumulator.add(t);
            }
        });

        //只有在driver程序中,可以调用accumulator的value方法,获取其值
        System.out.println(accumulator.value());

        sc.close();
    }

}
