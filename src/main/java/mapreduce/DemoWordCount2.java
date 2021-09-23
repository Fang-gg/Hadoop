package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

//切片
public class DemoWordCount2 {
    public static class WordCountMapper extends Mapper<LongWritable, Text,Text,LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split(","); //以逗号进行切分
            for (String s : split) {
                context.write(new Text(s),new LongWritable(1));
            }
        }
    }
    public static class WordCountReduce extends Reducer<Text,LongWritable,Text,LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0l;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(key,new LongWritable(sum));
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //创建一个job任务
        Job job = Job.getInstance();
        //指定job名称
        job.setJobName("第二个mapreduce程序，单词统计");
        //构建mapreduce
        //指定当前main所在类名(识别具体的类)
        job.setJarByClass(DemoWordCount2.class);
        //指定Map端的类
        job.setMapperClass(WordCountMapper.class);
        //指定Map端输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //指定Reduce端的类
        job.setReducerClass(WordCountReduce.class);
        //指定Reduce端输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //指定输入路径
        Path in = new Path("/word");
        FileInputFormat.addInputPath(job,in);
        //指定输出路径
        Path out = new Path("/out");       //是因为输出的路径是不能提前存在的
        //如果路径存在，就删除
        FileSystem fs = FileSystem.get(new Configuration());
        if(fs.exists(out)){
            fs.delete(out,true);
        }
        FileOutputFormat.setOutputPath(job,out);
        //启动任务
        job.waitForCompletion(true);
        System.out.println("mapreduce正在执行");
    }
}
