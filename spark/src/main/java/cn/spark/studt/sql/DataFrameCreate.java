package cn.spark.studt.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * 使用json文件创建DataFrame
 */
public class DataFrameCreate {


    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("DataFrameCreate");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame dataFrame = sqlContext.read().json("hdfs://hadoop7:9000/spark/data/students.json");

        //把数据打印出来
        dataFrame.show();

    }

}
