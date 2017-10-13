package cn.spark.studt.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
* 通过并行化集合创建RDD
 * parallelize,有一个重要的参数可以指定，就是要将集合切分成多少个partition。
 * spark会为每一个partition运行一个task来进行处理。
 * spark官方的建议是：为了集群中没个CPU创建2-4个partition，spark会根据集群的情况开设置partition的数据，但是也可以在调用parallelize()
 * 方法时，传入第二个参数，来设置RDD的partition数量，比如parallelize(arr,10)
 */
public class ParallelizeCollection {


    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName(ParallelizeCollection.class.getSimpleName())
                .setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
        JavaRDD<Integer> javaRDD = javaSparkContext.parallelize(list);

        Integer result = javaRDD.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.println("结果累加值为："+result);


        //关闭javasparkcontext
        javaSparkContext.close();
    }
}
