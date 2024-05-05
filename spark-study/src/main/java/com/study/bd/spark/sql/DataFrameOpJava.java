package com.study.bd.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

/**
 * DataFrame常见操作
 *
 * @author lx
 * @date 2024/05/05
 */
public class DataFrameOpJava {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");

        // 创建SparkSession对象，里面包含SparkContext和SqlContext
        SparkSession sparkSession = SparkSession.builder()
                .appName("DataFrameOpJava")
                .config(sparkConf)
                .getOrCreate();

        // 读取json文件，获取DataSet
        Dataset<Row> ds = sparkSession.read().json("D:\\developCode\\bigdata\\spark-study\\src\\main\\resources\\student.json");

        //打印schema信息
        ds.printSchema();

        // 展示几条数据
        ds.show(2);

        // 指定输出列
        ds.select("name", "age").show();

        //在select的时候可以对数据做一些操作 例如对age+1,需要引入import static org.apache.spark.sql.functions.col;
        ds.select(col("name"), col("age").plus(1)).show();

        //对数据进行过滤
        ds.filter(col("age").gt(18)).show();
        ds.where(col("age").gt(18)).show();

        //对数据进行分组，并计算对应的count
        ds.groupBy("age").count().show();

        sparkSession.stop();

    }
}
