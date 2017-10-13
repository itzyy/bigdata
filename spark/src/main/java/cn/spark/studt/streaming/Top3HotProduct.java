package cn.spark.studt.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * 与spark sql整合使用,top3热门商品实时统计
 * 日志格式 leo phone mobile_phone
 */
public class Top3HotProduct {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(Top3HotProduct.class.getSimpleName())
                .setMaster("local[2]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        JavaReceiverInputDStream<String> socketDatas = jsc.socketTextStream("hadoop7",9999);

        JavaPairDStream<String, Integer> productClickCountRDD = socketDatas.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(
                        s.split(" ")[1] + "-" + s.split(" ")[2],
                        1
                );
            }
        }).reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }, Durations.seconds(60), Durations.seconds(10));
        productClickCountRDD.print();
        productClickCountRDD.foreachRDD(new Function<JavaPairRDD<String, Integer>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, Integer> wordRDD) throws Exception {
                //组装数据到javaRDD<Row>
                JavaRDD<Row> rowRDD = wordRDD.map(new Function<Tuple2<String, Integer>, Row>() {
                    @Override
                    public Row call(Tuple2<String, Integer> wordCount) throws Exception {
                        System.out.println(wordCount._1+"============"+wordCount._2);
                        return RowFactory.create(
                                wordCount._1.split("-")[0],
                                wordCount._1.split("-")[1],
                                wordCount._2
                        );
                    }
                });

                List<StructField> fieldList = new ArrayList<StructField>();
                fieldList.add(DataTypes.createStructField("category",DataTypes.StringType,true));
                fieldList.add(DataTypes.createStructField("product",DataTypes.StringType,true));
                fieldList.add(DataTypes.createStructField("clickCount",DataTypes.IntegerType,true));

                StructType structType = DataTypes.createStructType(fieldList);

                HiveContext hc = new HiveContext(wordRDD.context());

                DataFrame clickDF = hc.createDataFrame(rowRDD, structType);

                //将60秒内的每个种类的商品的点击次数的数据,注册为一个临时表
                clickDF.registerTempTable("product_click_log");

                DataFrame queryResultRDD = hc.sql(
                        "select category,product,clickCount " +
                                " from (" +
                                "select " +
                                " category," +
                                "product," +
                                "clickCount," +
                                "row_number() over (partition by category order by clickCount desc) rank " +
                                " from product_click_log" +
                                ") tmp where rank<=3"
                );

                //这里应该将数据保存到redis缓存中,或者是muysql db中
                queryResultRDD.show();
                return null;
            }
        });

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }


}
