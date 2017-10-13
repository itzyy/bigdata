package cn.spark.studt.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

/**
 * 通常的load和save操作
 * 提供不同的save mode，save操作并不会执行所操作，并且不是原子的，因此是有一定风险出现脏数据的。
 * SaveMode.ErrorIfExists (默认)  如果目标位置已经存在数据，那么抛出一个异常
 * SaveMode.Append  如果目标位置已经存在数据，那么将数据追加进去
 * SaveMode.Overwrite   如果目标位置已经存在数据，那么就将已经存在的数据删除，用新数据进行覆盖
 * SaveMode.Ignore  如果目标位置已经存在数据，那么就忽略，不做任何操作
 */
public class GenericLoadSave {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(GenericLoadSave.class.getSimpleName())
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);
        long startTime = System.currentTimeMillis();
        DataFrame peopleDF = sqlContext.read().json("hdfs://hadoop7:9000/spark/data/people.json");
        System.out.println("json加载时间======"+(System.currentTimeMillis()-startTime));
        peopleDF.show();

        peopleDF.select("name","age").write().mode(SaveMode.Append.Overwrite).save("hdfs://hadoop7:9000/spark/data/nameAndAge.parquet");


        startTime = System.currentTimeMillis();
        DataFrame load = sqlContext.read().load("hdfs://hadoop7:9000/spark/data/nameAndAge.parquet");
        System.out.println("parquet加载时间======="+(System.currentTimeMillis()-startTime));
        load.show();


    }
}
