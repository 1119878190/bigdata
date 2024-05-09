package com.study.bd.spark.sqlonhive;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 *
 * spark写入数据到hive   sparkSql执行 insert into  ---推荐
 * @author lx
 * @date 2024/05/09
 */
public class SparkSqlWriteHive_3 {


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


        sparkSession.sql("insert into student_score_back values (4,'ssss','odod',5)");

        sparkSession.stop();
    }
}
