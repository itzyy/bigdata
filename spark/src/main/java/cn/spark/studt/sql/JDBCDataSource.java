package cn.spark.studt.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 通过JDBC求出
 * JDBC数据源
 * 首先，通过sqlcontext的read系列方法，将mysql中的数据加载为DataFrame
 */
public class JDBCDataSource {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName(JDBCDataSource.class.getSimpleName())
                .setMaster("local");

        final JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc.sc());

        Properties properties = new Properties();
        properties.setProperty("user", "root");
        properties.setProperty("password", "Mysql123,./");
        //传递url、tablename、username、password
        DataFrame student_infosDF = sqlContext.read().jdbc("jdbc:mysql://hadoop7:3306/mysql", "student_infos", properties);
        //studentDF 数据源
        DataFrame student_scoresDF = sqlContext.read().jdbc("jdbc:mysql://hadoop7:3306/mysql", "student_scores", properties);


        JavaRDD<Row> filter_student_info_score = student_infosDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>(row.getString(0), row.getInt(1));
            }
        }).join(
                student_scoresDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Row row) throws Exception {
                        return new Tuple2<String, Integer>(row.getString(0), row.getInt(1));
                    }
                })
                //将数据整合到Row中
        ).map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {
            @Override
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> s) throws Exception {
                return RowFactory.create(
                        s._1, Integer.valueOf(s._2._1), Integer.valueOf(s._2._2)
                );
            }
            //过滤出成绩大于80的学生
        }).filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row row) throws Exception {
                if (row.getInt(2) >= 80)
                    return true;
                else
                    return false;
            }
        });


        //动态设置student_info_score schema
        List<StructField> field_student_info_score = new ArrayList<StructField>();
        field_student_info_score.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        field_student_info_score.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        field_student_info_score.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));

        StructType structType = DataTypes.createStructType(field_student_info_score);

        DataFrame df_student_info_score = sqlContext.createDataFrame(filter_student_info_score, structType);

        Row[] rows = df_student_info_score.collect();
        for (Row row : rows) {
            System.out.println(row);
        }

        //大头来了，将DataFrame里面的数据插入到mysql中
        //这种方式在企业中很常见，有可能是插入到mysql,有可能是插入到hbase,还有可能是插入到redis中，
        df_student_info_score.javaRDD().foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                String sql = "insert into good_student_info_scores values ('" +
                        row.getString(0) + "'," +
                        row.getInt(1) + "," +
                        row.getInt(2)+")";
                System.out.println("=============="+sql);
                Connection conn = null;
                Statement stat = null;
                try {
                    Class.forName("com.mysql.jdbc.Driver");
                    conn = DriverManager.getConnection(
                            "jdbc:mysql://hadoop7:3306/mysql",
                            "root",
                            "Mysql123,./"
                    );
                    stat = conn.createStatement();
                    stat.executeUpdate(sql);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    stat.close();
                    conn.close();
                }
            }
        });
        sc.close();
    }
}
