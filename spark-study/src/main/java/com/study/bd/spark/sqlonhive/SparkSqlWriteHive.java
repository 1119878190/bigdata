package com.study.bd.spark.sqlonhive;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * spark写入数据到hive  insertInto
 * <p>
 * 官方文档 https://spark.apache.org/docs/2.4.0/sql-data-sources-hive-tables.html
 *
 * @author lx
 * @date 2024/05/09
 */
public class SparkSqlWriteHive {

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

        // 官方建议提前在hive中创建表，在sparksql中直接使用
        // 注意，通过sparksql创建hive表的时候，如果想要指定存储格式等参数(默认TextFile),则必须使用using hive
        // 例如：  create tabel t1(id int) using hive OPTIONS(fileFormat 'parquet')
        sparkSession.sql("CREATE TABLE IF NOT EXISTS " +
                "student_score_back (id INT, name STRING,sub STRING,score INT) USING hive " +
                "OPTIONS (fileFormat 'textfile',fieldDelim ',')");


        dataset.write()
                .mode("overWrite")
                .insertInto("student_score_back");


        sparkSession.stop();
    }

}
