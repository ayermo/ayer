package com.example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Main {
    public static class task1map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(","); // 根据CSV文件的分隔符来拆分行

      
            String target = tokens[tokens.length - 1];

            // 输出<标签, 1>键值对
            context.write(new Text(target), one);
        }
    }
    public static class task1reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;

            // 统计相同标签的数量
            for (IntWritable val : values) {
                sum += val.get();
            }

            // 输出结果 <标签, 数量>
            context.write(key, new IntWritable(sum));
        }
    }
    public static void main(String[] args) throws Exception {//主方法，函数入口
        Configuration conf = new Configuration();           //实例化配置文件类
        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "task1");             //实例化Job类
        job.setInputFormatClass(TextInputFormat.class);     //指定使用默认输入格式
        job.setJarByClass(Main.class);               //设置主类名
        job.setMapperClass(task1map.class);        //指定使用上述自定义Map类
        job.setMapOutputKeyClass(Text.class);            //指定Map类输出的，K类型
        job.setMapOutputValueClass(IntWritable.class);     //指定Map类输出的，V类型
        job.setReducerClass(task1reduce.class);         //指定使用上述自定义Reduce类
        job.setOutputKeyClass(Text.class);                //指定Reduce类输出的,K类型
        job.setOutputValueClass(IntWritable.class);               //指定Reduce类输出的,V类型
        job.setOutputFormatClass(TextOutputFormat.class);  //指定使用默认输出格式类
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));    //设置输出结果文件位置
        System.exit(job.waitForCompletion(true) ? 0 : 1);    //提交任务并监控任务状态
    }
}
