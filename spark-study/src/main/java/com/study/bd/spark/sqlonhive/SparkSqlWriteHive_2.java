package com.study.bd.spark.sqlonhive;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * spark写入数据到hive  saveAsTable  ---不推荐
 *
 * @author lx
 * @date 2024/05/09
 */
public class SparkSqlWriteHive_2 {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");

        // 创建SparkSession对象，里面包含SparkContext和SqlContext
        SparkSession sparkSession = SparkSession.builder()
                .appName("SparkSqlReadHive")
                .config(sparkConf)
                // 开启对hive的支持
                .enableHiveSupport()
                .config("spark.sql.warehouse.dir", "hdfs://cdh03:8020/user/hive/warehouse")
                .getOrCreate();


        Dataset<Row> dataset = sparkSession.sql("select * from student_score");

        /**
         * 分为两种情况，表不存在和表存在
         * 1∶表不存在，则会根据DataFrame中的schema自动创建目标表并写入数据
         * 2:表存在
         * 2.1:如果mode=append,当DataFrame中的schema和表中的schema相同(字段顺序可以不同),则执行追加操作。
         * 当DataFrame中的schema和表中的schema不相同，则报错。
         * 2.2:如果mode=overwrite,当DataFrame 中的schema和表中的Schema相同(字段顺序可以不同),则直接覆盖。
         * 当DataFrame 中的schema和表中的schema不相同,则会删除之前的表,然后按照DataFrame中的schema重新创建表
         *
         */


        // 表不存在
        dataset.write()
                .mode("overWrite")
                // 这里需要指定数据格式 ；parquet  orc avro  json csv text 。不指定默认是parquet
                // 注意text数据格式在这里不支持int数据类型
                // parquet 和 orc 可以正常使用
                .format("parquet")
                .saveAsTable("student_score_back");

        // 表存在
        dataset.write()
                // 指定数据写入格式 append追加  overwrite覆盖
                .mode("append")
                // 这里需要指定数据格式 ；parquet  orc avro  json csv text 。不指定默认是parquet
                // 注意text数据格式在这里不支持int数据类型
                // 针对已存在的表，当mode为append时，这里必须指定为hive。
                // 针对已存在的表，当mode为overwrite时，如果指定为hive，则会生成默认存储格式(TextFile)的hive表。
                // 也就是说 这里的format尽量与建表语句中的 fileForma一致
                // parquet 和 orc 可以正常使用
                .format("hive")
                .saveAsTable("student_score_back");


        sparkSession.stop();
    }

}
