package cn.spark.studt.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * 使用编译的方式将RDD转换成DataFrame
 * 场景：同一份数据可以组成多份信息
 */
public class RDD2DataFrameaProgrammatically {


    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(RDD2DataFrameaProgrammatically.class.getSimpleName())
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);

        //第一步：创建一个RDD
        JavaRDD<String> javaRDD = sc.textFile("C:\\Users\\LENOVO\\Desktop\\students.txt");

        //使用RowFactory创建row
        //注意：什么格式的数据，就使用什么格式，不要都当做string类型
        JavaRDD<Row> rowRDD = javaRDD.map(new Function<String, Row>() {
            @Override
            public Row call(String s) throws Exception {
                String[] stus = s.split(",");
                return RowFactory.create(
                        Integer.valueOf(stus[0]),
                        stus[1],
                        Integer.valueOf(stus[2])
                );
            }
        });


        //第二步：动态构造元数据（创建表，必须需要元数据）

        ArrayList<StructField> strutsTypes = new ArrayList<>();
        strutsTypes.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
        strutsTypes.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        strutsTypes.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(strutsTypes);

        //第三步：使用动态构造的元数据，生成DataFrame
        DataFrame studentDF = sqlContext.createDataFrame(rowRDD, structType);

        //注册一个临时表
        studentDF.registerTempTable("student");

        //获取sql查询结果
        DataFrame teenager = sqlContext.sql("select * from student where age<=18");

        //DataFrame转换成RDD
        List<Row> collect = teenager.javaRDD().collect();

        for (Row row :
                collect) {
            System.out.println(row);
        }


    }

}
