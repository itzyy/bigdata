package cn.kepuchina.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by zouyy on 2017/7/10.
 */
public class WordCount {

    private static Logger logger =Logger.getLogger(WordCount.class);

    public static void main(String[] args){

        if(args.length<2){
            logger.error(" args need at least 2");
            System.exit(2);
        }
        try {
            Configuration configuration = new Configuration();
            Job job = Job.getInstance(configuration,"WordCount");
            job.setJarByClass(WordCount.class);
            job.setMapperClass(CleanerMapper.class);
            job.setReducerClass(CleanerReduce.class );
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

            for(int i =0;i<args.length-1;i++){
                FileInputFormat.addInputPath(job,new Path(args[i]));
            }
            FileOutputFormat.setOutputPath(job,new Path(args[args.length-1]));
            System.exit(job.waitForCompletion(true)?0:1);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static class CleanerMapper extends Mapper<LongWritable,Text,Text,LongWritable> {

        String line ="";
        String[] strs={};
        Text result =new Text();
        LongWritable counts = new LongWritable();

        @Override
        public void map(LongWritable key ,Text value,Mapper<LongWritable,Text,Text,LongWritable>.Context context){

            try{
                line = value.toString();
                strs = line.split("\t");
                result.set(strs[0]);
                counts.set(1L);
                context.write(result,counts);
            }catch (IOException e){
                logger.error(e.getMessage());
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
        }
    }
    public static class CleanerReduce extends Reducer<Text,LongWritable,Text,LongWritable>{


        String danci="";

        Text resultKey =new Text();
        LongWritable resultValue = new LongWritable();

        @Override
        public void reduce(Text key ,Iterable<LongWritable> values,Context context){
            try{
            danci = key.toString();
            int count=0;
            for(LongWritable value : values){
                count++;
            }
            resultKey.set(danci);
            resultValue.set(count);
            context.write(resultKey,resultValue);
            }catch (IOException e){
                logger.error(e.getMessage());
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
        }

    }

}
