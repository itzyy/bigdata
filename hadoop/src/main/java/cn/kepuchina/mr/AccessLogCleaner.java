package cn.kepuchina.mr;

import org.apache.avro.JsonProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 * Created by LENOVO on 2017/7/11.
 */
public class AccessLogCleaner {

    private static Logger logger = Logger.getLogger(AccessLogCleaner.class);

    public static void main(String[] args){
        if(args.length<2){
            logger.error(" args length error");
            System.exit(2);
        }
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration,"AccessLogCleaner");
            job.setJarByClass(AccessLogCleaner.class);
            job.setMapperClass(AccessLogMapper.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            for (int i =0 ;i<args.length-1;i++){
                FileInputFormat.addInputPath(job,new Path(args[i]));
            }
            FileOutputFormat.setOutputPath(job,new Path(args[args.length-1]));
            System.exit(job.waitForCompletion(true)?0:1);
        } catch (IOException e) {
            logger.error(e.getMessage());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }


    public static class AccessLogMapper extends Mapper<LongWritable,Text,NullWritable,Text>{

        private DateFormat dateFormat ;
        private String line;
        private String[] strs=null;
        private Text resultValue =new Text();

        @Override
        protected void setup(Mapper<LongWritable,Text,NullWritable,Text>.Context context) throws IOException, InterruptedException {
            logger = Logger.getLogger(AccessLogMapper.class);
            dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            dateFormat=null;
        }

        @Override
        public void map(LongWritable key,Text value, Mapper<LongWritable,Text,NullWritable,Text>.Context context) throws IOException, InterruptedException {
            if (value!=null&&value.getLength()>0){
                line = value.toString();
                strs = line.split("\t");
                if(strs!=null&& strs.length==10){
                    String time = dateFormat.format(new Date(Long.parseLong(strs[9])));
                    //装载进入对象，实现数据清理
                    AccessLog accessLog = new AccessLog(strs[0],strs[2],strs[3],strs[4],strs[6],strs[7],time);

                    //ip字段解析
                    //通过redis配置地域数据源

                    //解释request访问
                    if(strs[5]!=null&&strs[5].split(" ").length==3){
                        accessLog.setMethod(strs[5].split(" ")[0]);
                        accessLog.setPath(strs[5].split(" ")[1]);
                        accessLog.setHttp_version(strs[5].split(" ")[2]);
                    }

                    resultValue.set(accessLog.toString());
                    context.write(NullWritable.get(),resultValue);
                }

            }else{
                logger.info(AccessLogMapper.class.getName()+"\t map阶段行内容错误！");
            }
        }
    }
}
