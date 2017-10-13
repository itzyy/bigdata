package cn.spark.studt.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;

/**
 * 需求：
 * 1、筛选出符合查询条件（城市、平台、版本）的数据
 * 2、统计出每天搜索uv排名前3的搜索词
 * 3、按照每天的top3搜索词的uv搜索总次数，倒序排序
 * 4、将数据保存到hive中
 */
public class DailyTop3KeyWord {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName(DailyTop3KeyWord.class.getSimpleName())
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext hc = new HiveContext(sc.sc());
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

        //组织查询条件
        Map<String, List<String>> searchs = new HashMap<String, List<String>>();
        searchs.put("address", Arrays.asList("beijing"));
        searchs.put("platform", Arrays.asList("android"));
        searchs.put("version", Arrays.asList("1.0", "1.1", "1.2"));

        //将查询条件进行共享，
        final Broadcast<Map<String, List<String>>> broadcast = sc.broadcast(searchs);

        //读取hdfs中访问日志
        JavaRDD<String> initRDD = sc.textFile("hdfs://hadoop7:9000/spark/data/keyword.txt");

        //筛选出符合条件的数据
        JavaRDD<String> filterTop3RDD = initRDD.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                String[] keyword = s.split("\t");
                Map<String, List<String>> searchData = broadcast.value();

                if (keyword[3] != null && !searchData.get("address").contains(keyword[3])) {
                    return false;
                }
                if (keyword[4] != null && !searchData.get("platform").contains(keyword[4])) {
                    return false;
                }
                if (keyword[5] != null && searchData.get("version").size() > 0 && !searchData.get("version").contains(keyword[5])) {
                    return false;
                }
                return true;
            }
        });
        //组装成map类型，key=date-keyword value =1
        JavaPairRDD<String, String> mapTop2RDD = filterTop3RDD.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<String, String>(s.split("\t")[0] + ":" + s.split("\t")[2], s.split("\t")[1]);
            }
        });

        //对数据进行分组
        JavaPairRDD<String, Iterable<String>> groupTop3RDD = mapTop2RDD.groupByKey();
        //根据分组后的key，进行去重，对value进行统计
        JavaPairRDD<String, Integer> dateKeywordUvRDD = groupTop3RDD.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Iterable<String>> s) throws Exception {
                String date_keyword = s._1;
                Iterator<String> users = s._2.iterator();
                List<String> distinctUsers = new ArrayList<String>();
                while (users.hasNext()) {
                    String user = users.next();
                    if (!distinctUsers.contains(user)) {
                        distinctUsers.add(user);
                    }
                }
                int count = distinctUsers.size();
                return new Tuple2<String, Integer>(date_keyword, count);
            }
        });

        JavaRDD<Row> rowUvRDD = dateKeywordUvRDD.map(new Function<Tuple2<String, Integer>, Row>() {
            @Override
            public Row call(Tuple2<String, Integer> s) throws Exception {
                return RowFactory.create(
                        s._1.split(":")[0],
                        s._1.split(":")[1],
                        s._2()
                );
            }
        });


        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("date", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("keyword", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("uv", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(structFields);

        DataFrame df = hc.createDataFrame(rowUvRDD, structType);

        df.registerTempTable("daily_user_uv");

        //需要使用hivecontext 调用row_number ，使用sqlcontext会出现莫名错误。
        String sql = "select date,keyword,uv from (" +
                "select" +
                " date," +
                " keyword," +
                " uv," +
                "row_number() over (partition by date order by uv desc) rank " +
                "from daily_user_uv" +
                ") tmp_sales where rank<=3";
        DataFrame dailyTop3KeywordDF = hc.sql(sql);

        dailyTop3KeywordDF.show();

        //开始对每天的top3的搜索量总次数汇总，然后倒序排序
        JavaRDD<Row> sumTop3SortRDD = dailyTop3KeywordDF.javaRDD().mapToPair(new PairFunction<Row, String, String>() {
            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                //数据格式：<date,keyword:uv>
                return new Tuple2<String, String>(row.getString(0), row.getString(1) + ":" + row.getInt(2));
            }
        })
                .groupByKey()
                .mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<String, Iterable<String>> s) throws Exception {
                        String date = s._1;
                        Iterator<String> keyword_uvs = s._2.iterator();
                        Long uv_count = 0l;
                        while (keyword_uvs.hasNext()) {
                            String value = keyword_uvs.next();
                            Long uv = Long.valueOf(value.split(":")[1]);
                            uv_count += uv;
                            date += "," + value;
                        }
                        //数据格式：<uv_count,date,keyword:uv,keyword:uv,keyword:uv...>
                        return new Tuple2<Long, String>(uv_count, date);
                    }
                })
                //倒序
                .sortByKey(false)
                //flatmap和map的区别，在于返回值Iterable<Row>和row
                .flatMap(new FlatMapFunction<Tuple2<Long, String>, Row>() {
                    @Override
                    public Iterable<Row> call(Tuple2<Long, String> s) throws Exception {
                        String values = s._2;
                        String[] keywords = values.split(",");
                        String date = keywords[0];
                        List<Row> rows = new ArrayList<Row>();
                        rows.add(
                                RowFactory.create(
                                        date,
                                        keywords[1].split(":")[0],
                                        Integer.valueOf(keywords[1].split(":")[1])
                                )
                        );
                        rows.add(
                                RowFactory.create(
                                        date,
                                        keywords[2].split(":")[0],
                                        Integer.valueOf(keywords[2].split(":")[1])
                                )
                        );
                        rows.add(
                                RowFactory.create(
                                        date,
                                        keywords[3].split(":")[0],
                                        Integer.valueOf(keywords[3].split(":")[1])
                                )
                        );
                        return rows;
                    }
                });
        hc.createDataFrame(sumTop3SortRDD,structType).saveAsTable("daily_top3_keyword_uv");

    }

}
