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

/**
 * 这个是用来计算男女各有多少。数据是我自己电脑上准备好的。
 */
public class DemoSexSum3 {
    public static class SexMapper extends Mapper<LongWritable, Text,Text,LongWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String sex = line.split(",")[3];
            context.write(new Text(sex),new LongWritable(1));
        }
    }
    public static class SexReduce extends Reducer<Text,LongWritable,Text,LongWritable>{
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
        //指定reduce节点个数
        job.setNumReduceTasks(2);
        //指定job名称
        job.setJobName("第三个mapreduce程序，性别统计");
        //构建mapreduce
        //指定当前main所在类名(识别具体的类)
        job.setJarByClass(DemoSexSum3.class);
        //指定Map端的类
        job.setMapperClass(SexMapper.class);
        //指定Map端输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //指定Reduce端的类
        job.setReducerClass(SexReduce.class);
        //指定Reduce端输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //指定输入路径
        Path in = new Path("/data/student");
        FileInputFormat.addInputPath(job,in);
        //指定输出路径
        Path out = new Path("/output");       //是因为输出的路径是不能提前存在的
        //如果路径存在，就删除
        FileSystem fs = FileSystem.get(new Configuration());
        if(fs.exists(out)){
            fs.delete(out,true);
        }
        FileOutputFormat.setOutputPath(job,out);
        //启动任务
        job.waitForCompletion(true);
        System.out.println("DemoSexSum正在执行");
    }
}
