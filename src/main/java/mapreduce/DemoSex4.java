package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 这代码就是不需要进行Reduce计算，只需要统计出性别是男的。
 */
public class DemoSex4 {
    public static class SexMapper extends Mapper<LongWritable, Text,Text,NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String sex = line.split(",")[3];
            if(sex.equals("男")){
                context.write(value, NullWritable.get());
            }
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //创建一个job任务
        Job job = Job.getInstance();
        /**
         * 有些情况下,不需要reduce(聚合程序),
         * 在不需要聚合操作的时候,可以不需要reduce
         * 而reduce默认为1,需要手动设置为0,
         * 如果没有设置为0,会产生默认的reduce,只不过reduce不处理任何数据
         */
        //指定job名称
        job.setJobName("第四个mapreduce程序");
        //构建mapreduce
        //指定当前main所在类名(识别具体的类)
        job.setJarByClass(DemoSex4.class);
        //指定Map端的类
        job.setMapperClass(SexMapper.class);
        //指定Map端输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
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
        System.out.println("DemoSex正在执行");
    }
}
