package cn.spark.studt.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

/**
 * row_number() 开窗函数实战
 */
public class RowNumberWindowFunction {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(RowNumberWindowFunction.class.getSimpleName())
                .setMaster("local");
        //指定hive版本
        conf.set("spark.sql.hive.metastore.version", "0.14.0");
        //加载hive.metastore所需的jar
        conf.set("spark.sql.hive.metastore.jars", "" +
                "/usr/local/hadoop-2.6.0/share/hadoop/common/*:" +
                "/usr/local/hadoop-2.6.0/share/hadoop/common/lib/*:" +
                "/usr/local/hadoop-2.6.0/share/hadoop/hdfs/*:" +
                "/usr/local/hadoop-2.6.0/share/hadoop/hdfs/lib/*:" +
                "/usr/local/hadoop-2.6.0/share/hadoop/mapreduce/*:" +
                "/usr/local/hadoop-2.6.0/share/hadoop/mapreduce/lib/*:" +
                "/usr/local/hadoop-2.6.0/share/hadoop/yarn/*:" +
                "/usr/local/hadoop-2.6.0/share/hadoop/yarn/lib/*:" +
                "/usr/local/hive-0.14/lib/*");
        JavaSparkContext sc = new JavaSparkContext(conf);

        HiveContext hc = new HiveContext(sc.sc());

        hc.sql("DROP TABLE IF EXISTS sales");
        hc.sql("CREATE TABLE IF NOT EXISTS sales (" +
                "product STRING," +
                "category STRING," +
                "revenue BIGINT) row format delimited fields terminated by '\t' lines terminated by '\n'");
        hc.sql("LOAD DATA LOCAL INPATH '/opt/work/data/sales.txt' INTO TABLE sales");

        String sql = "select product,category,revenue from (" +
                        "select" +
                        " product," +
                        "category," +
                        "revenue," +
                        "row_number() over (partition by category order by revenue) rank " +
                        "from sales" +
                    ") tmp_sales where rank<=3";
        System.out.println("=================" + sql);
        DataFrame topDF = hc.sql(sql);
        topDF.show();
        hc.sql("DROP TABLE IF EXISTS top3_sales");
        topDF.saveAsTable("top3_sales");


    }


}
