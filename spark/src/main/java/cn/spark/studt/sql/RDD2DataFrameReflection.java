package cn.spark.studt.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

/**
 * 使用反射的方式将RDD转换成DataFrame
 * 场景：已经知道该数据的固定的schema
 */
public class RDD2DataFrameReflection {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("RDD2DataFrameReflection")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<Student> javaRDD = sc.textFile("C:\\Users\\LENOVO\\Desktop\\students.txt")
                .map(new Function<String, Student>() {
                    @Override
                    public Student call(String s) throws Exception {
                        String[] strs = s.split(",");
                        Student student = new Student(Integer.valueOf(strs[0]), strs[1], Integer.valueOf(strs[2]));
                        return student;
                    }
                });

        //使用反射方式，将RDD转换为DataFrame
        //将student.class传入进去，其实就是用反射方式来创建DataFrame
        //因为Student.class本身就是一个反射的应用
        //然后底层还得通过student class进行反射应用，来获取其中的field
        //这里要求，JavaBean必须实习那Serializable接口，是可序列的
        DataFrame studentDf = sqlContext.createDataFrame(javaRDD,Student.class);

        //拿到一个DataFrame之后，就可以将其注册为一个临时表，然后针对其中的数据执行SQL语句
        studentDf.registerTempTable("student");

        //针对student临时表执行SQL语句，查询年龄小于等于18岁的学生，就是teemageer
        DataFrame teenagerDF = sqlContext.sql("select * from student where age<=18");

        //反射生成的RDD列的顺序与实际文件中的顺序不相同
        teenagerDF.show();

        //针对student临时表执行SQL语句，查询年龄小于等于18岁的学生，就是teenageer
        JavaRDD<Row> teenagerRDD = teenagerDF.javaRDD();

        //将RDD中的数据，进行映射，映射为Student
        List<Student> students = teenagerRDD.map(new Function<Row, Student>() {
            @Override
            public Student call(Row row) throws Exception {
                Student student = new Student(row.getInt(1), row.getString(2), row.getInt(0));
                return student;
            }
        }).collect();

        for (Student student :
                students) {
            System.out.println(student.toString());
        }




    }

}
