package cn.spark.studt.sql;

import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

/**
 * spark sql支持对Hive中存储的数据进行读写，操作对象为HiveContext
 * 使用HiveContext.可以执行Hive的大部分功能，包括创建表、往表里导入数据以及查询SQL语句查询表中的数据，查询出来的数据是一个数组
 * 将hive-site.xml拷贝到spark/conf下，将mysql connetor拷贝到spark/lib目录下
 */
public class HiveOperation {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(HiveOperation.class.getSimpleName());
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
                "/usr/local/hive-0.14/lib/*"
        );

        JavaSparkContext sc = new JavaSparkContext(conf);

        //创建HiveContext,这里它接收的是SparkContext作为参数，不是JavaSparkContext
        HiveContext hc = new HiveContext(sc.sc());

        //第一个功能，执行hive的hql语句
        //判断student_infos是否存在，存在就删除
        hc.sql("DROP TABLE IF EXISTS student_infos");
        //创建student_infos
        hc.sql("CREATE TABLE IF NOT EXISTS student_infos (name STRING,age int) row format delimited fields terminated by '\t' lines terminated by '\n'");
        //将hdfs的学生数据导入到hive中student_infos
        hc.sql("LOAD DATA LOCAL INPATH '/opt/work/data/student_infos.txt' OVERWRITE INTO TABLE student_infos");

        //判断student_scores是否存在
        hc.sql("DROP TABLE IF EXISTS student_scores");
        //创建student_scores
        hc.sql("CREATE TABLE IF NOT EXISTS student_scores (name STRING,score INT) row format delimited fields terminated by '\t' lines terminated by '\n'");
        hc.sql("LOAD DATA LOCAL INPATH '/opt/work/data/student_scores.txt' OVERWRITE INTO TABLE student_scores");

        //student_infos、student_scores进行join，获取成绩大于80的学生
        DataFrame goodStudentDF = hc.sql("select si.name,si.age,ss.score from student_infos si join student_scores ss on si.name=ss.name where ss.score >=80");

        hc.sql("DROP TABLE IF EXISTS good_student_infos");
        //将DataFrame中的数据保存的永久性表中
        goodStudentDF.saveAsTable("good_student_infos");

        //查询成绩大于80的结果表，个人觉得，直接使用goodstudentDF.javaRDD()
        long startTime = System.currentTimeMillis();
        Row[] good_student_infos = hc.table("good_student_infos").collect();
        for (Row goodstudentRow : good_student_infos){
            System.out.println(goodstudentRow);
        }
        System.out.println("hc.table========="+(System.currentTimeMillis()-startTime));

        //测试：使用JavaRDD循环，要比使用hiveContext.table要快
        startTime = System.currentTimeMillis();
        JavaRDD<Row> rowJavaRDD = goodStudentDF.javaRDD();
        rowJavaRDD.foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                System.out.println(row);
            }
        });
        System.out.println("javaRDD.foreach"+(System.currentTimeMillis()-startTime));

        sc.close();

    }


}
