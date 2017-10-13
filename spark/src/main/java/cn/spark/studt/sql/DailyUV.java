package cn.spark.studt.sql;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.Struct;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 根据每天的用户访问日志，统计每日的uv
 */
public class DailyUV {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(DailyUV.class.getSimpleName())
                .setMaster("local");



        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc.sc());
        //生成数据
        List<String> datas = Arrays.asList(
                "2015-10-10 1120",
                "2015-10-10 1120",
                "2015-10-10 1120",
                "2015-10-11 5622",
                "2015-10-11 5622",
                "2015-10-11 5622",
                "2015-10-12 8956",
                "2015-10-12 8956",
                "2015-10-12 8956"

        );
        JavaRDD<String> rdd_gen_data = sc.parallelize(datas);

        JavaRDD<Row> rdd_row = rdd_gen_data.map(new Function<String, Row>() {
            @Override
            public Row call(String s) throws Exception {
                return RowFactory.create(
                        s.split(" ")[0],
                        s.split(" ")[1]
                );
            }
        });

        List<StructField> structTypes = new ArrayList<StructField>();
        structTypes.add(DataTypes.createStructField("date",DataTypes.StringType,true));
        structTypes.add(DataTypes.createStructField("userid",DataTypes.StringType,true));
        StructType structType = DataTypes.createStructType(structTypes);

        DataFrame dataFrame = sqlContext.createDataFrame(rdd_row, structType);
        dataFrame.groupBy("date","userid").agg(new Column("date"),new Column("userid")).distinct().count();


    }
}
