package com.study.bd.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 *
 * 通过反射的方式将RDD转换为DataFrame
 *
 * @author lx
 * @date 2024/05/05
 */
public class RddToDataFrameByReflectJava {

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

        JavaRDD<Student> stuRDD = rdd.map(new Function<Tuple2<String, Integer>, Student>() {
            @Override
            public Student call(Tuple2<String, Integer> tup) throws Exception {
                return new Student(tup._1, tup._2);
            }
        });

        // 将RDD转为dataFrame   注意：Student这个类必须声明为public，并且必须实现序列化
        Dataset<Row> dataFrame = sparkSession.createDataFrame(stuRDD, Student.class);
        dataFrame.createOrReplaceTempView("student");

        //执行sql查询
        Dataset<Row> df2 = sparkSession.sql("select name, age from student where age > 18");
        df2.show();

        //将DataFrame转化为RDD，注意：这里需要转为JavaRDD
        JavaRDD<Row> resRDD = df2.javaRDD();

        List<Student> resList = resRDD.map(new Function<Row, Student>() {
            @Override
            public Student call(Row row) throws Exception {
                return new Student(row.getAs("name").toString(), Integer.parseInt(row.getAs("age").toString()));
            }
        }).collect();
        for(Student stu: resList){
            System.out.println(stu);
        }
        sparkSession.stop();


    }
}
