package cn.spark.studt.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * sparrk json文件与传统以上的json是不一样的，每行都必须只能包含一个完整的json对象
 * 例子：查询成绩为80分以上的学生的基本信息与成绩信息
 */
public class JSONDataSource {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(JSONDataSource.class.getSimpleName())
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);

        //读取student_info学生信息
        DataFrame dataFrame = sqlContext.read().json("hdfs://hadoop7:9000/spark/data/students.json");
        dataFrame.queryExecution();

        //注册student_info临时表
        dataFrame.registerTempTable("student_info");

        // 将查询到的结果注入到dataframe
        DataFrame filterStuDF = sqlContext.sql("select name,age from student_info");

        filterStuDF.show();
        JavaRDD<Row> studentInfoRDD = filterStuDF.javaRDD();

        //使用并行集合创建学生成绩表
        List<String> studentScores = new ArrayList<String>();
        studentScores.add("{\"name\":\"leo\",\"score\":90}");
        studentScores.add("{\"name\":\"jack\",\"score\":95}");
        studentScores.add("{\"name\":\"marry\",\"score\":102}");
        JavaRDD<String> parStuRDD = sc.parallelize(studentScores);

        DataFrame filterDF = sqlContext.read().json(parStuRDD);
        //注册临时学生分数表
        filterDF.registerTempTable("student_scores");

        DataFrame df = sqlContext.sql("select name,score from student_scores");

        //将student_info和student_score进行join
        JavaPairRDD<String, Tuple2<Integer, Integer>> joinRDD = studentInfoRDD.mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>(row.getString(0),Integer.valueOf(String.valueOf(row.getLong(1))));
            }
        }).join(df.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>(row.getString(0), Integer.valueOf(String.valueOf(row.getLong(1))));
            }
        }));

        //将关联的数据装载到row中
        //先创建row
        JavaRDD<Row> rowRDD = joinRDD.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {
            @Override
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> s) throws Exception {
                return RowFactory.create(s._1, Integer.valueOf(s._2._1), Integer.valueOf(s._2._2));
            }
        });

        //在创建StructField，构造元数据
        List<StructField> structFields = new ArrayList<StructField>();

        structFields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("score",DataTypes.IntegerType,true));

        StructType t_student_info_score = DataTypes.createStructType(structFields);

        //生成查询的结果注入到DataFrame
        DataFrame df_student_info_score = sqlContext.createDataFrame(rowRDD, t_student_info_score);

        //为筛选出来的结果创建临时表
        df_student_info_score.registerTempTable("student_info_score");

        DataFrame q_student_info_score = sqlContext.sql("select name ,age,score from student_info_score");
        q_student_info_score.explain();

        //将筛选出来的信息，保存的到parquet/json中，上传到hdfs
        q_student_info_score.write().mode(SaveMode.Overwrite).save("hdfs://hadoop7:9000/spark/data/good_students/");
        q_student_info_score.write().format("json").mode(SaveMode.Overwrite).save("hdfs://hadoop7:9000/spark/data/good_students/");



    }

}
