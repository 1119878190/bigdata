package com.study.bd.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 * 通过编程的方式将RDD转为DataFrame
 *
 *
 * @author lx
 * @date 2024/05/05
 */
public class RddToDataFrameByProgramJava {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");

        // 创建SparkSession对象，里面包含SparkContext和SqlContext
        SparkSession sparkSession = SparkSession.builder()
                .appName("DataFrameSqlJava")
                .config(sparkConf)
                .getOrCreate();

        //获取SparkContext
        //从sparkSession中获取的是scala中的sparkContext，所以需要转换成java中的sparkContext
        JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        Tuple2<String, Integer> t1 = new Tuple2<>("jack", 18);
        Tuple2<String, Integer> t2 = new Tuple2<>("tom", 20);
        Tuple2<String, Integer> t3 = new Tuple2<>("jess", 30);
        JavaRDD<Tuple2<String, Integer>> rdd = sparkContext.parallelize(Arrays.asList(t1, t2, t3));

        // 组装rdd
        JavaRDD<Row> rowRDD = rdd.map(new Function<Tuple2<String, Integer>, Row>() {
            @Override
            public Row call(Tuple2<String, Integer> tup) throws Exception {
                return RowFactory.create(tup._1, tup._2);
            }
        });

        // 指定元数据信息
        List<StructField> structFieldList = new ArrayList<>();
        structFieldList.add(DataTypes.createStructField("name", DataTypes.StringType,true));
        structFieldList.add(DataTypes.createStructField("age", DataTypes.IntegerType,true));

        StructType schema = DataTypes.createStructType(structFieldList);

        // 构建DataFrame
        Dataset<Row> dataFrame = sparkSession.createDataFrame(rowRDD, schema);

        dataFrame.createOrReplaceTempView("student");
        Dataset<Row> resDf = sparkSession.sql("select name, age from student where age > 18");

        JavaRDD<Row> resRDD = resDf.javaRDD();

        List<Tuple2<String, Integer>> resList = resRDD.map(new Function<Row, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {

                return new Tuple2<>(row.getString(0), row.getInt(1));
            }
        }).collect();

        for (Tuple2<String, Integer> tup : resList) {
            System.out.println(tup);
        }

        sparkSession.stop();

    }
}
