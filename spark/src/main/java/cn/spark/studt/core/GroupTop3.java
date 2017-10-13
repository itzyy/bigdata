package cn.spark.studt.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by Zouyy on 2017/9/11.
 */
public class GroupTop3 {


    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName(GroupTop3.class.getSimpleName())
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaRDD<String> javaRDD = sc.textFile("C:\\Users\\LENOVO\\Desktop\\grouptop.txt");

        JavaPairRDD<String, String> javaPairRDD = javaRDD.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<String, String>(s.split("\t")[0], s.split("\t")[1]);
            }
        });

        JavaPairRDD<String, Iterable<String>> groupByKey = javaPairRDD.groupByKey();

        JavaPairRDD<String,  Iterable<Integer>> pairRDD = groupByKey.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, Iterable<Integer>>() {
            @Override
            public Tuple2<String,  Iterable<Integer>> call(Tuple2<String, Iterable<String>> s) throws Exception {
                String key = s._1;
                Iterator<String> iterator = s._2.iterator();
                Integer[] top3 = new Integer[3];
                while (iterator.hasNext()) {
                    String next = iterator.next();
                    for (int i = 0; i < 3; i++) {
                        if (top3[i] == null) {
                            //请初始化这个数组
                            top3[i] = Integer.valueOf(next);
                            //数据进入到top3中以后,说明完成一次数据操作,需要立即跳出
                            break;
                        } else if (top3[i] < Integer.valueOf(next)) {
                            for (int j = 2; j > i; j--) {
                                //对top3数组进行数据移位
                                top3[j] = top3[j - 1];
                            }
                            top3[i] = Integer.valueOf(next);
                            break;
                        }
                    }
                }
                return new Tuple2<String, Iterable<Integer>>(key,Arrays.asList(top3));
            }
        });

        pairRDD.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> s) throws Exception {
                System.out.println(s._1+"========"+s._2);
            }
        });
    }


}
