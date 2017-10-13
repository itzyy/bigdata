package cn.spark.studt.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * Parquet 数据源之之使用编程方式加载数据
 * 例子：查询出hdfs中name列进行打印
 */
public class ParquetLoanData {


    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(ParquetLoanData.class.getSimpleName())
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);

        DataFrame studentDF = sqlContext.read().parquet("hdfs://hadoop7:9000/spark/data/users.parquet");

        //创建临时表
        studentDF.registerTempTable("users");

        DataFrame filterUser = sqlContext.sql("select name from users");

        filterUser.javaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return "name:"+row.getString(0);
            }
        }).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }

}
