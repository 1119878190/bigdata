package com.study.bd.spark.sqlonhive;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 *
 * 通过spark sql 操作 hive
 * @author lx
 * @date 2024/05/07
 */
public class SparkSqlReadHive {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");

        // 创建SparkSession对象，里面包含SparkContext和SqlContext
        SparkSession sparkSession = SparkSession.builder()
                .appName("SparkSqlReadHive")
                .config(sparkConf)
                .enableHiveSupport()
                .config("spark.sql.warehouse.dir","hdfs://cdh03:8020/user/hive/warehouse")
                .getOrCreate();


//        System.setProperty("hadoop.home.dir","D:\\developSoft\\hadoop-3.2.0");
        sparkSession.sql("select * from t1").show();

        sparkSession.stop();

    }
}
