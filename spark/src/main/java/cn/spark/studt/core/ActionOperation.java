package cn.spark.studt.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by Zouyy on 2017/9/7.
 */
public class ActionOperation {

    public static void main(String[] args) {

//        reduce();
//        collect();
//        count();
//        take();
//        saveAsTextFile();
        countByKey();
    }

    /**
     * reduce action : 将RDD中的元素进行聚合操作，第一个和第二个元素聚合，在和第三个聚合，在和第四个聚合，以此类推
     */
    public static void reduce(){

        SparkConf conf = new SparkConf()
                .setAppName(ActionOperation.class.getSimpleName())
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8,9,10);

        JavaRDD<Integer> javaRDD = sc.parallelize(numbers);

        Integer reduce = javaRDD.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.println("汇总值："+reduce);

    }

    /**
     * collect action : 将RDD中所有元素获取到本地客户端
     * 一般不建议使用,如果rdd中的数据量比较大的话,从远程走大量的网络传输,将数据获取到本地,除了性能差外,容易发生oom异常,内存溢出
     */
    public static void collect(){
        SparkConf conf = new SparkConf()
                .setAppName(ActionOperation.class.getSimpleName())
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8,9,10);

        JavaRDD<Integer> javaRDD = sc.parallelize(numbers);

        List<Integer> collect = javaRDD.collect();
        for (Integer num : collect){
            System.out.println(num);
        }
        sc.close();
    }
    /**
     * count action : 获取RDD元素总数,相当与获取集合的size
     */
    public static void count(){
        SparkConf conf = new SparkConf()
                .setAppName(ActionOperation.class.getSimpleName())
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8,9,10);

        JavaRDD<Integer> javaRDD = sc.parallelize(numbers);

        long count = javaRDD.count();
        System.out.println(count);
        sc.close();
    }
    /**
     * take action : 获取RDD中前N个元素
     * 与collect类似，但是collect是获取全部元素，take只是获取前N个元素
     */
    public static void take(){
        SparkConf conf = new SparkConf()
                .setAppName(ActionOperation.class.getSimpleName())
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8,9,10);
        JavaRDD<Integer> javaRDD = sc.parallelize(numbers);
        List<Integer> take = javaRDD.take(3);
        for (Integer num : take){
            System.out.println(num);
        }
        sc.close();
    }

    /**
     * saveAsFile action : 将RDD元素保存到文件中,对每个元素调用toString 方法
     */
    public static void saveAsTextFile(){
        SparkConf conf = new SparkConf()
                .setAppName(ActionOperation.class.getSimpleName())
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8,9,10);
        JavaRDD<Integer> javaRDD = sc.parallelize(numbers);

        //生成一个test.txt目录,且不能存在,里面有数据文件
        javaRDD.saveAsTextFile("L:\\test.txt");

        sc.close();
    }

    /**
     * countByKey action : 对每个key对应的值进行count计数
     */
    public static void countByKey(){
        SparkConf conf = new SparkConf()
                .setAppName(ActionOperation.class.getSimpleName())
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String, Integer>> classs = Arrays.asList(
                new Tuple2<String, Integer>("class1", 11),
                new Tuple2<String, Integer>("class1", 11),
                new Tuple2<String, Integer>("class2", 11),
                new Tuple2<String, Integer>("class3", 11),
                new Tuple2<String, Integer>("class3", 11),
                new Tuple2<String, Integer>("class2", 11)
        );
        JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(classs);

        Map<String, Object> maps = pairRDD.countByKey();

        for (Map.Entry<String,Object> map:maps.entrySet()){
            System.out.println(map.getKey()+"========"+map.getValue());
        }


    }
}
