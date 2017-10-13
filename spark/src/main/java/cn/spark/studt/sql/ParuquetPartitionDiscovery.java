package cn.spark.studt.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Parquet自动推断分区
 * 例子：自动推断用户数数据的性别和国家
 */
public class ParuquetPartitionDiscovery {


    public static void main(String[] args) {


        SparkConf conf = new SparkConf()
                .setAppName(ParuquetPartitionDiscovery.class.getSimpleName())
                .setMaster("local");
        //关闭自动推断分区类型
//        conf.set("spark.sql.sources.partitionColumnTypeInference.enabled","false");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);

        DataFrame userDF = sqlContext.read().parquet("hdfs://hadoop7:9000/users/gender=0/country=US/users.parquet");

        userDF.registerTempTable("user");

        DataFrame sql = sqlContext.sql("select * from user");
        sql.printSchema();
        sql.show();


    }

}
