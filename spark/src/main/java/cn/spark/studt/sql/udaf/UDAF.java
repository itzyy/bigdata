package cn.spark.studt.sql.udaf;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Zouyy on 2017/9/19.
 */
public class UDAF {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(UDAF.class.getSimpleName())
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        List<String> datas = Arrays.asList("xiaoming", "xiaoming", "feifei", "feifei", "feifei","katong");

        JavaRDD<String> initRDD = sc.parallelize(datas);

        JavaRDD<Row> rowRDD = initRDD.map(new Function<String, Row>() {
            @Override
            public Row call(String s) throws Exception {
                return RowFactory.create(
                        s
                );
            }
        });

        List<StructField> structTypes = Lists.newArrayList();
        structTypes.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        StructType structType = DataTypes.createStructType(structTypes);

        DataFrame namesDF = sqlContext.createDataFrame(rowRDD, structType);
        namesDF.registerTempTable("names");

        sqlContext.udf().register("countString",new StringCount());

        List<Row> rows = sqlContext.sql("select name,countString(name) from names group by name").javaRDD().collect();

        for(Row row : rows){
            System.out.println(row);
        }
        sc.close();
    }
}
