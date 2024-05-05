package com.study.bd.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * DataFrame执行sql语句
 *
 * @author lx
 * @date 2024/05/05
 */
public class DataFrameSqlJava {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");

        // 创建SparkSession对象，里面包含SparkContext和SqlContext
        SparkSession sparkSession = SparkSession.builder()
                .appName("DataFrameSqlJava")
                .config(sparkConf)
                .getOrCreate();

        Dataset<Row> ds = sparkSession.read().json("D:\\developCode\\bigdata\\spark-study\\src\\main\\resources\\student.json");

        // 将Dataset注册为一个临时表
        ds.createOrReplaceTempView("student");

        // 使用sql查询临时表中的数据
        sparkSession.sql("select age,count(*) as num from student group by age")
                .show();

        sparkSession.stop();

    }
}
