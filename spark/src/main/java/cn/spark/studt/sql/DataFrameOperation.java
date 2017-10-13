package cn.spark.studt.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * DataFrame的常用操作
 */
public class DataFrameOperation {


    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("DataFrameOpeation")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);

        //创建DataFrame
        DataFrame df = sqlContext.read().json("hdfs://hadoop7:9000/spark/data/students.json");

        //打印DataFrame中的所有数据(select * from )
        df.show();
        //打印DataFrame中元数据（Schema）
        df.printSchema();
        // 查询某列所有的数据
        df.select("name").show();
        // 根据某一列所有的数据，并对列进行计算
        // plus 对数据进行+1
        df.select(df.col("name"),df.col("age").plus(1)).show();
        //对某一列的值进行过滤
        df.filter(df.col("age").gt(18)).show();
        //根据某一列进行分组，然后进行聚合
        df.groupBy(df.col("age")).count().show();

    }

}
