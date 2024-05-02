package com.study.bd.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 批量导入
 * <p>
 * 2.利用BulkLoad 在map阶段，把数据封装成put操作。将数据生产HBase的底层存储文件File
 * <p>
 * <p>
 * a       c1      name    zs
 * a       c1      age     18
 * b       c1      name    ls
 * b       c1      age     29
 * c       c1      name    ww
 * c       c1      age     31
 *
 * @author lx
 * @date 2024/05/02
 */
public class BatchImportBulkLoad {


    /**
     * @author lx
     * @date 2024/05/02
     */
    public static class BulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context) throws IOException, InterruptedException {
            // 读取文件中的数据
            String[] strs = value.toString().split("\t");
            if (strs.length == 4) {
                String rowkey = strs[0];
                String columnFamily = strs[1];
                String name = strs[2];
                String val = strs[3];
                ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable(rowkey.getBytes());
                Put put = new Put(rowkey.getBytes());
                put.addColumn(columnFamily.getBytes(), name.getBytes(), val.getBytes());
                context.write(immutableBytesWritable, put);
            }
        }
    }


    public static void main(String[] args) throws Exception {
        // hdfs上的文件位置
        String inputPatch = "hdfs://cdh03:8020/tmp/hbase_import.dat";
        // 输出hfile的路径
        String outputPath = "hdfs://cdh03:8020/tmp/hbase_out";
        // 输出到hbase的表名
        String outTableName = "batch2";

        // 指定job需要的配置参数
        Configuration configuration = new Configuration();
        configuration.set("hbase.table.name", outTableName);
        configuration.set("hbase.zookeeper.quorum", "cdh03:2181,cdh02:2181,cdh01:2181");

        // 创建一个job
        Job job = Job.getInstance(configuration, "Batch Import Hbase Table" + outTableName);
        // 注意：这一行必须设置，否则在集群中执行的时候是找不到启动类这个类的
        job.setJarByClass(BatchImportBulkLoad.class);

        // 指定输入的路径（可以是文件，也可以是目录）
        FileInputFormat.setInputPaths(job, new Path(inputPatch));

        // 指定输出路径[如果输出路径存在，就将其删除]
        FileSystem fileSystem = FileSystem.get(configuration);
        Path output = new Path(outputPath);
        if (fileSystem.exists(output)) {
            fileSystem.delete(output, true);
        }
        // 指定输出路径
        FileOutputFormat.setOutputPath(job, output);

        // 指定map相关代码
        job.setMapperClass(BulkLoadMapper.class);
        // 指定输出的k2的类型
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        // 指定输出的v2的类型
        job.setMapOutputValueClass(Put.class);
        job.setOutputFormatClass(HFileOutputFormat2.class);

        // 将map阶段的结果输出到hbase中
        TableMapReduceUtil.initTableReducerJob(outTableName, null, job);
        TableMapReduceUtil.addDependencyJars(job);

        // 禁用 reduce 如果只是数据的过滤 那么可以只有map阶段,不用reduce阶段,那么就setNumReduceTasks(0)
        job.setNumReduceTasks(0);

        Connection connection = ConnectionFactory.createConnection(configuration);
        TableName tableName = TableName.valueOf(outTableName);
        HFileOutputFormat2.configureIncrementalLoad(job, connection.getTable(tableName), connection.getRegionLocator(tableName));


        // 提交job
        job.waitForCompletion(true);
    }
}
