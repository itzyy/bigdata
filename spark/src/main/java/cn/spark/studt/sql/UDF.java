package cn.spark.studt.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Zouyy on 2017/9/19.
 */
public class UDF {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("UDF")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        //生成数据
        List<String> datas = Arrays.asList(
                "2015-10-10 1120 zouyy",
                "2015-10-10 1120 zouyy",
                "2015-10-10 1120 zouyy",
                "2015-10-11 5622 baojuyi",
                "2015-10-11 5622 baojuyi",
                "2015-10-11 5622 baojuyi",
                "2015-10-12 8956 liuyijun",
                "2015-10-12 8956 liuyijun",
                "2015-10-12 8956 liuyijun"
        );
        //并行化集合创建
        JavaRDD<String> namesDF = sc.parallelize(datas);

        JavaRDD<String> javaRDD = sc.parallelize(datas);

        JavaRDD<Row> rdd = javaRDD.map(new Function<String, Row>() {
            @Override
            public Row call(String s) throws Exception {
                return RowFactory.create(
                        s.split(" ")[0],
                        s.split(" ")[1],
                        s.split(" ")[2]
                );
            }
        });

        List<StructField> list = new ArrayList<StructField>();
        list.add(DataTypes.createStructField("date",DataTypes.StringType,true));
        list.add(DataTypes.createStructField("userid",DataTypes.StringType,true));
        list.add(DataTypes.createStructField("username",DataTypes.StringType,true));
        StructType structTypes = DataTypes.createStructType(list);

        DataFrame dataFrame = sqlContext.createDataFrame(rdd, structTypes);

        dataFrame.registerTempTable("kp_uv");

        sqlContext.udf().register("strLen", new UDF1<String, Integer>() {

            @Override
            public Integer call(String s) throws Exception {
                return s.length();
            }
        }, DataTypes.IntegerType);

        List<Row> rows = sqlContext.sql("select date,userid,strLen(username) from kp_uv").javaRDD().collect();
        for (Row row :
                rows) {
            System.out.println(row);

        }

    }
}
