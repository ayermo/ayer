package com.example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Main2 {
    public static class task2map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(","); // 根据CSV文件的分隔符来拆分行

      
            String target = tokens[25];

            // 输出<标签, 1>键值对
            context.write(new Text(target), one);
        }
    }
    public static class task2reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
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
    public static class IntWritableDecreasingComparator extends WritableComparator {
        protected IntWritableDecreasingComparator() {
            super(IntWritable.class, true);
        }
    
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            IntWritable intWritableA = (IntWritable) a;
            IntWritable intWritableB = (IntWritable) b;
            return -1 * intWritableA.compareTo(intWritableB);
        }
    }
    
    
    
    public static void main(String[] args) throws Exception {//主方法，函数入口
        Configuration conf = new Configuration();           //实例化配置文件类
        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "task2");             //实例化Job类
        job.setInputFormatClass(TextInputFormat.class);     //指定使用默认输入格式
        job.setJarByClass(Main2.class);               //设置主类名
        job.setMapperClass(task2map.class);        //指定使用上述自定义Map类
        job.setMapOutputKeyClass(Text.class);            //指定Map类输出的，K类型
        job.setMapOutputValueClass(IntWritable.class);     //指定Map类输出的，V类型
        job.setReducerClass(task2reduce.class);         //指定使用上述自定义Reduce类
        job.setOutputKeyClass(Text.class);                //指定Reduce类输出的,K类型
        job.setOutputValueClass(IntWritable.class);               //指定Reduce类输出的,V类型
        job.setOutputFormatClass(TextOutputFormat.class);  //指定使用默认输出格式类
        job.setSortComparatorClass(IntWritableDecreasingComparator.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));    //设置输出结果文件位置
        System.exit(job.waitForCompletion(true) ? 0 : 1);    //提交任务并监控任务状态
    }
}