package com.study.bd.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * 批量导入:
 * 1.利用MapReduce中分装好的方法，在map阶段，把数据分装成Put操作，直接将数据入库
 *
 *
 * a       c1      name    zs
 * a       c1      age     18
 * b       c1      name    ls
 * b       c1      age     29
 * c       c1      name    ww
 * c       c1      age     31
 *
 *
 *
 * @author lx
 * @date 2024/05/02
 */
public class BatchImportMR {


    public static class BatchImportMapper extends Mapper<LongWritable, Text, NullWritable, Put> {

        /**
         * map阶段  输入k1,v1 输出 k2，v2
         *
         * @param key     k1 行号
         * @param value   v1 值
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, Put>.Context context) throws IOException, InterruptedException {

            // 读取文件中的数据
            String[] strs = value.toString().split("\t");
            if (strs.length == 4) {
                String rowkey = strs[0];
                String columnFamily = strs[1];
                String name = strs[2];
                String val = strs[3];
                Put put = new Put(rowkey.getBytes());
                put.addColumn(columnFamily.getBytes(), name.getBytes(), val.getBytes());
                context.write(NullWritable.get(), put);
            }

        }
    }


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        // hdfs上的文件位置
        String input = "hdfs://cdh03:8020/tmp/hbase_import.dat";
        // 输出到hbase的表名
        String outTableName = "batch1";

        // 指定job需要的配置参数
        Configuration configuration = new Configuration();
        configuration.set("hbase.table.name",outTableName);
        configuration.set("hbase.zookeeper.quorum", "cdh03:2181,cdh02:2181,cdh01:2181");

        // 创建一个job
        Job job = Job.getInstance(configuration, "Batch Import Hbase Table" + outTableName);
        // 注意：这一行必须设置，否则在集群中执行的时候是找不到WordCountJob这个类的
        job.setJarByClass(BatchImportMR.class);

        // 指定输入的路径（可以是文件，也可以是目录）
        FileInputFormat.setInputPaths(job, new Path(input));

        // 指定map相关代码
        job.setMapperClass(BatchImportMapper.class);
        // 指定k2的类型
        job.setMapOutputKeyClass(NullWritable.class);
        // 指定v2的类型
        job.setMapOutputValueClass(Put.class);

        // 将map阶段的结果输出到hbase中
        TableMapReduceUtil.initTableReducerJob(outTableName,null,job);
        TableMapReduceUtil.addDependencyJars(job);


        // 如果只是数据的过滤 那么可以只有map阶段,不用reduce阶段,那么就setNumReduceTasks(0)
        job.setNumReduceTasks(0);


        // 提交job
        job.waitForCompletion(true);

    }

}
