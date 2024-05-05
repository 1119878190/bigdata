package com.study.bd.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 需求： 使用json文件创建DataFrame
 * @author lx
 * @date 2024/05/05
 */
public class SqlDemoJava {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");

        // 创建SparkSession对象，里面包含SparkContext和SqlContext
        SparkSession sparkSession = SparkSession.builder()
                .appName("SqlDemoJava")
                .config(sparkConf)
                .getOrCreate();

        // 读取json文件，获取DataSet
        Dataset<Row> stuDF = sparkSession.read().json("D:\\developCode\\bigdata\\spark-study\\src\\main\\resources\\student.json");

        stuDF.show();

        sparkSession.stop();

    }
}
