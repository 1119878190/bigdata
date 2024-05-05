package com.study.bd.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 *
 *  load  save
 *
 * @author lx
 * @date 2024/05/05
 */
public class LoadAndSaveOpJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        //创建SparkSession对象，里面包含SparkContext和SqlContext
        SparkSession sparkSession = SparkSession.builder().config(conf).appName("LoadAndSaveOpJava").getOrCreate();

        //读取数据
        Dataset<Row> df = sparkSession.read().format("json").load("D:\\developCode\\bigdata\\spark-study\\src\\main\\resources\\student.json");

        //保存数据
        df.select("name", "age")
                .write()
                .format("csv")
                .mode(SaveMode.Append) // saveMode
                .save("hdfs://cdh03:8020/tmp/out-save002");

        sparkSession.stop();

    }
}
