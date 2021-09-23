package mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

/**
 * 这个代码是将两张表做连接之后查询。
 * 条件是学号一样。
 * 数据是我自己的数据，代码思路基本都一样。
 */
public class DemoJoin {
    public static class JoinMapper extends Mapper<LongWritable, Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //1.获取数据的路径 InputSplit
            //通过上下文 context  上面是hdfs 下面如果有reduce就是reduce，没有就是hdfs
            InputSplit inputSplit = context.getInputSplit();
            FileSplit fs = (FileSplit)inputSplit;
            String url = fs.getPath().toString();
            //2.判断
            if(url.contains("student")){//如果判断成功，则当前数据为student
                String id = value.toString().split(",")[0];
                //为了方便reduce端数据的操作，故针对不同的数据打一个不同的标签
                String line ="*" + value.toString();
                context.write(new Text(id),new Text(line));
            }else{//要是false 当前数据为score
                //以学号作为K 也是这两张表的关联条件
                String id = value.toString().split(",")[0];
                //为了方便reduce端数据的操作，故针对不同的数据打一个不同的标签
                String line ="#" + value.toString();
                context.write(new Text(id),new Text(line));
            }
        }
    }
    public static class JoinReduce extends Reducer<Text,Text,Text, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //数据在循环之外保存
            String stuInfo = "";
            ArrayList<String> scores = new ArrayList<>();
            for (Text value : values) {
                //获取一行一行的数据(所有数据都包含student和score)
                String line = value.toString();
                if(line.startsWith("*")){//true 为学生数据
                    stuInfo = line.substring(1);
                }else{//false 为学生成绩
                    scores.add(line.substring(1));
                }
            }
            //数据拼接
            for (String score : scores) {
                String subject = score.split(",")[1];
                String s = score.split(",")[2];
                String end=stuInfo+","+subject+","+s;
                context.write(new Text(end),NullWritable.get());
            }
        }
    }
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("Join MapReduce");
        job.setJarByClass(DemoJoin.class);

        job.setMapperClass(JoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(JoinReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //指定路径
        FileInputFormat.addInputPath(job,new Path("/data"));
        FileOutputFormat.setOutputPath(job,new Path("/join"));
        job.waitForCompletion(true);
        System.out.println("join 正在执行");
    }
}
