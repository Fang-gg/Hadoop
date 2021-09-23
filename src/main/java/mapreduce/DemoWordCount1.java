package mapreduce;
//用来统计文件中单词个数
// 重写 覆盖mapreduce框架中的map()方法和reduce()方法

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

public class DemoWordCount1 {
    //map类
    //第一队k，v，是决定数据的输入格式
    //第二队k，v，是决定数据的输出格式
    /**
    * KEYIN
    *    ---->k1 表示每一行的起始位置(偏移量offset)
    * VALUEIN
    *    ---->V1 表示每一行的文本内容
    * KEYOUT
    *    ---->k2 表示每一行中的每个单词
    * VALUEOUT
    *    ---->V2 表示每一行中的每个单词的出现次数，固定值为1
    * */
    public static class WordCountMapper extends Mapper<LongWritable, Text,Text,LongWritable>{
        /*
        * map阶段数据是一行一行过来的
        * 每一行数据都需要执行代码
        * */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException,   InterruptedException {
            //通过Context输出 Text(一整行数据)，1
            String line = value.toString();
            context.write(new Text(line),new LongWritable(1));
        }
    }
    //reduce类
    //用来接收map端输出的数据
    public static class WordCountReduce extends Reducer<Text,LongWritable,Text,LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            /**
             * reduce 聚合程序 每一个k都会调用一次
             * 默认是一个节点
             * key:每一个单词
             * values:map端 当前k所对应的所有的v
             */
            long sum = 0l;
            for (LongWritable value : values) {
                   sum += value.get();
            }
            // 把计算结果输出到hdfs
            context.write(key,new LongWritable(sum));
        }
    }

    /**
     * 是当前mapreduce程序入口
     * 用来构建mapreduce程序
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //创建一个job任务
        Job job = Job.getInstance();
        //指定job名称
        job.setJobName("第一个mapreduce程序，单词统计");
        //构建mapreduce
        //指定当前main所在类名(识别具体的类)
        job.setJarByClass(DemoWordCount1.class);
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
