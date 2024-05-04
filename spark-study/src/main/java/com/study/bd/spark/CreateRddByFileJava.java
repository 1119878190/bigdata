package com.study.bd.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * 通过文件创建RDD---计算文件内数据的长度
 *
 * @author lx
 * @date 2024/05/04
 */
public class CreateRddByFileJava {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("CreateRddByFileJava")
                .setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        // 本地路径或hdfs文件路径
        String path = "D:\\tmp\\testData\\words.txt";
        path = "hdfs://cdh03:8020/tmp/words.txt";
        JavaRDD<String> rdd = javaSparkContext.textFile(path, 2);
        // 获取每一行数据的长度
        JavaRDD<Integer> lengthRDD = rdd.map(new Function<String, Integer>() {
            @Override
            public Integer call(String line) throws Exception {
                return line.length();
            }
        });

        // 计算文件内数据的总长度
        Integer lengthReduce = lengthRDD.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.println(lengthReduce);
        javaSparkContext.stop();
    }
}
