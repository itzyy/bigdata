package cn.spark.studt.core;

import groovy.lang.Tuple;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Int;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Zouyy on 2017/9/7.
 */
public class TranformationOperation {

    public static void main(String[] args) {
        //使用map将集合中每个元素乘以2
       // map();
        //过滤出集合中的偶数
        //filter();
        //拆分行为单个的单词
//        flatMap();
//        groupByKey();
//        reduceBykey();
//        sortByKey();
        join();
//        cogroup();
    }

    /**
     * map算子案例：使用map将集合中每个元素乘以2
     */
    public static void map(){
        //创建sparkconf
        SparkConf conf = new SparkConf()
                .setAppName(TranformationOperation.class.getSimpleName())
                .setMaster("local");
        //创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        //构造集合
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        //并行化集合，创建初始RDD
        JavaRDD<Integer> javaRDD = sc.parallelize(numbers);

        //使用map 进入自定义函数
        JavaRDD<Integer> maps = javaRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v) throws Exception {
                return v * 2;
            }
        });

        //调用action进行输出
        maps.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer v) throws Exception {
                System.out.println(v);
            }
        });
        sc.close();
    }

    /**
     * filter算子案例：过滤出集合中的偶数
     */
    public static void filter(){
        //创建sparkconf
        SparkConf conf = new SparkConf()
                .setAppName(TranformationOperation.class.getSimpleName())
                .setMaster("local");


        //创建javasparkconf
        JavaSparkContext sc = new JavaSparkContext(conf);

        //构造集合
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        //并行化集合，创建初始RDD
        JavaRDD<Integer> javaRDD = sc.parallelize(numbers);

        //使用filter算子
        JavaRDD<Integer> filterRDD = javaRDD.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer v) throws Exception {
                //通过% 取余数
                return v % 2 == 0;
            }
        });

        //打印新的RDD
        filterRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

        //关闭
        sc.close();
    }

    /**
     * flatmap算子案例:返回一个或者多个新元素
     * 将每一行文本，拆分为多个单词
     */
    public static void flatMap(){

        SparkConf conf = new SparkConf()
                .setAppName(TranformationOperation.class.getSimpleName())
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> list = Arrays.asList("hello you", "hello me", " hello world");
        JavaRDD<String> javaRDD = sc.parallelize(list);
        JavaRDD<String> flatMapRDD = javaRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                //通过split分割出数组类型，并且通过Arrays转换成list
                return Arrays.asList(s.split(" "));
            }
        });
        flatMapRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        sc.close();

    }

    /**
     * groupByKey算子案例：按照班级对成绩进行分组
     */
    public static void groupByKey(){
        SparkConf conf = new SparkConf()
                .setAppName(TranformationOperation.class.getSimpleName())
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String, Integer>> classs = Arrays.asList(
                new Tuple2<String, Integer>("class1", 90),
                new Tuple2<String, Integer>("class2", 85),
                new Tuple2<String, Integer>("class1", 60),
                new Tuple2<String, Integer>("class2", 80)
        );

        //调用parallellize 进行简直安装
        JavaPairRDD<String, Integer> javaPairRDD = sc.parallelizePairs(classs);

        //使用groupByKey 将数据拆分为<key,values>;一个key对应多个values
        JavaPairRDD<String, Iterable<Integer>> pairRDD = javaPairRDD.groupByKey();

        pairRDD.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> s) throws Exception {
                System.out.println("class："+s._1);
                Iterator<Integer> iterator = s._2.iterator();
                while(iterator.hasNext()){
                    System.out.println(iterator.next());
                }
                System.out.println("=========================");
            }
        });
    }

    /**
     * 统计每个班级的总分
     * reduceByKey算子案例：对每个key对应的value进行reduce 聚合操作
     */
    public static void reduceBykey(){
        SparkConf conf = new SparkConf()
                .setAppName(TranformationOperation.class.getSimpleName())
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String, Integer>> classs = Arrays.asList(
                new Tuple2<String, Integer>("class1", 90),
                new Tuple2<String, Integer>("class2", 85),
                new Tuple2<String, Integer>("class1", 60),
                new Tuple2<String, Integer>("class2", 80)
        );

        JavaPairRDD<String, Integer> javaPairRDD = sc.parallelizePairs(classs);

        //第三个参数累计的结果
        JavaPairRDD<String, Integer> pairRDD = javaPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                /**
                 * 对每个key,都会将其value，一次传入方法，
                 * 从而聚合出每个key对应的一个value
                 * 然后，将每个key对应的一个value,组合成一个tuple2，作为一个新的RDD元素
                 */
                return v1 + v2;
            }
        });

        pairRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1+"===="+stringIntegerTuple2._2);
            }
        });

    }

    /**
     * j将学生分数进行排序
     * sortByKey算子排序：对每个key对应的value进行排序操作
     */
    public static void sortByKey(){
        SparkConf conf = new SparkConf()
                .setAppName(TranformationOperation.class.getSimpleName())
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer, String>> classs = Arrays.asList(
                new Tuple2<Integer, String>(85, "zouyy"),
                new Tuple2<Integer, String>(81,"baizhengjun"),
                new Tuple2<Integer, String>(90,"zhangzuoling"),
                new Tuple2<Integer, String>(80,"zhangxueyou")
        );
        JavaPairRDD<Integer, String> javaPairRDD = sc.parallelizePairs(classs);

        //通过key进行排序,true（默认）:降序；false：升序
        JavaPairRDD<Integer, String> pairRDD = javaPairRDD.sortByKey(false);

        pairRDD.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> s) throws Exception {
                System.out.println(s._1 + ":" + s._2);
            }
        });
        sc.close();
    }


    /**
     * 打印每个学生的成绩
     * join：对每个包含<key,value>对的RDD进行join操作，每个处理key join上的pair,都会传入自定义函数进行处理
     */
    public static void join(){
        SparkConf conf = new SparkConf()
                .setAppName(TranformationOperation.class.getSimpleName())
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer, String>> stus = Arrays.asList(
                new Tuple2<Integer, String>(1, "zouyy"),
                new Tuple2<Integer, String>(1, "zouyy123"),
                new Tuple2<Integer, String>(2, "zhangzuoling"),
                new Tuple2<Integer, String>(3, "zhangxueyou"),
                new Tuple2<Integer, String>(4, "baijuyi")
        );
        List<Tuple2<Integer, Integer>> scores = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 90),
                new Tuple2<Integer, Integer>(1, 100),
                new Tuple2<Integer, Integer>(2, 30),
                new Tuple2<Integer, Integer>(3, 80),
                new Tuple2<Integer, Integer>(4, 85)
        );

        JavaPairRDD<Integer, String> stuRDD = sc.parallelizePairs(stus);
        JavaPairRDD<Integer, Integer> scoresRDD = sc.parallelizePairs(scores);

        /**
         * 比如有(1, 1) (1, 2) (1, 3)的一个RDD
         // 还有一个(1, 4) (2, 1) (2, 2)的一个RDD
         // join以后，实际上会得到(1 (1, 4)) (1, (2, 4)) (1, (3, 4)),对应的类型为：<Integer, Tuple2<String, Integer>
         *join 通过key获取第一个匹配值后，会马上返回
         */
        JavaPairRDD<Integer, Tuple2<String, Integer>> joinRDD = stuRDD.join(scoresRDD);

        joinRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<String, Integer>> t) throws Exception {
                System.out.print("student'id:"+t._1);
                System.out.print("\t student'name:"+t._2._1);
                System.out.println("\t student'score:"+t._2._2);
            }
        });
    }


    /**
     * 打印每个学生的成绩
     * cogroup 与join不同的在与cogroup会获取所有匹配的值，而join只会获取最先匹配的
     */
    public static void cogroup(){
        SparkConf conf = new SparkConf()
                .setAppName(TranformationOperation.class.getSimpleName())
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer, String>> stus = Arrays.asList(
                new Tuple2<Integer, String>(1, "zouyy"),
                new Tuple2<Integer, String>(1, "zouyy"),
                new Tuple2<Integer, String>(2, "zhangzuoling"),
                new Tuple2<Integer, String>(3, "zhangxueyou"),
                new Tuple2<Integer, String>(4, "baijuyi")
        );
        List<Tuple2<Integer, Integer>> scores = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 90),
                new Tuple2<Integer, Integer>(2, 30),
                new Tuple2<Integer, Integer>(3, 80),
                new Tuple2<Integer, Integer>(4, 85),
                new Tuple2<Integer, Integer>(1, 100),
                new Tuple2<Integer, Integer>(2, 81),
                new Tuple2<Integer, Integer>(3, 65),
                new Tuple2<Integer, Integer>(4, 74)
        );
        JavaPairRDD<Integer, String> stuRDD = sc.parallelizePairs(stus);
        JavaPairRDD<Integer, Integer> scoresRDD = sc.parallelizePairs(scores);

        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> cogroupRDD = stuRDD.cogroup(scoresRDD);

        // 打印studnetScores RDD
        cogroupRDD.foreach(
                new VoidFunction<Tuple2<Integer,Tuple2<Iterable<String>,Iterable<Integer>>>>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t)
                            throws Exception {
                        System.out.println("student id: " + t._1);
                        System.out.println("student name: " + t._2._1);
                        System.out.println("student score: " + t._2._2);
                        System.out.println("===============================");
                    }
                });
        // 关闭JavaSparkContext
        sc.close();
    }
}
