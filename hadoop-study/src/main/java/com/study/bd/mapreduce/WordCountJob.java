package com.study.bd.mapreduce;

import org.apache.hadoop.conf.Configuration;
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
 * 需求： 读取hdfs上的文件 words.txt，计算文件中的每个单词出现的总次数
 * <p>
 * 最终需要的结果新式如下：
 * <p>
 * hello  2
 * me  1
 * you  1
 *
 * @author lx
 * @date 2024/01/30
 */
public class WordCountJob {


    /**
     * 组装job
     *
     * @param args
     */
    public static void main(String[] args) {

        if (args.length != 2){
            System.exit(100);
        }

        try {
            // 指定job需要的配置参数
            Configuration configuration = new Configuration();

            // 创建一个job
            Job job = Job.getInstance(configuration);
            // 注意：这一行必须设置，否则在集群中执行的时候是找不到WordCountJob这个类的
            job.setJarByClass(WordCountJob.class);

            // 指定输入的路径（可以是文件，也可以是目录）
            FileInputFormat.setInputPaths(job, new Path(args[0]));
            // 指定输出路径（只能指定一个不存在的目录）
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            // 指定map相关代码
            job.setMapperClass(MyMapper.class);
            // 指定k2的类型
            job.setMapOutputKeyClass(Text.class);
            // 指定v2的类型
            job.setMapOutputValueClass(LongWritable.class);

            // 如果只是数据的过滤 那么可以只有map阶段,不用reduce阶段,那么就setNumReduceTasks(0)
            //job.setNumReduceTasks(0);

            // 指定reduce相关的代码
            job.setReducerClass(MyReducer.class);
            // 指定k3的类型
            job.setOutputKeyClass(Text.class);
            // 指定v3的类型
            job.setOutputValueClass(LongWritable.class);

            // 提交job
            job.waitForCompletion(true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }


    /**
     * Map阶段
     */
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {


        /**
         * 需要实现map函数
         * 接受<,1,v1> 输出<k2,v2>
         *
         * @param k1      输入key
         * @param v1      输入value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable k1, Text v1, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {

            // k1代表的是每一行数据的行首偏移量，v1代表的是每一行的内容
            // 对获取到的每一行数据进行切割，把单词切割出来
            String[] words = v1.toString().split("\t");
            for (String word : words) {
                // 把切割出来的单词分装成 <k2,v2>
                Text k2 = new Text(word);
                LongWritable v2 = new LongWritable(1L);
                // 把<k2,v2>写出去
                context.write(k2, v2);
            }

        }
    }


    /**
     * Reducer阶段
     */
    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        /**
         * 针对 <k2,{v2....}>的数据进行累加求和，并把最终的结果转换为k3,v3写出去
         *
         * @param k2      k2
         * @param v2s     v2s
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text k2, Iterable<LongWritable> v2s, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {

            long sum = 0;
            for (LongWritable v2 : v2s) {
                sum += v2.get();
            }
            // 组装 k3,v3
            Text k3 = k2;
            LongWritable v3 = new LongWritable(sum);
            context.write(k3, v3);

        }
    }


}
