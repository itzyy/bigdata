package cn.spark.studt.core;

import org.apache.hadoop.fs.Hdfs;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * Created by Zouyy on 2017/9/5.
 */
public class HdfsFile {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf()
                .setAppName(HdfsFile.class.getSimpleName())
                ;



        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> textFile = sc.textFile("hdfs://hadoop7:9000/spark/data/spark.txt");
//        JavaRDD<String> textFile = sc.textFile("C:\\Users\\LENOVO\\Desktop\\spark.txt");


        JavaPairRDD<String, Integer> pairRDD = textFile.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> javaPairRDD = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        javaPairRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> s) throws Exception {
                System.out.println(s._1+"\t"+s._2);
            }
        });
    }
}
