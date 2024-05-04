package com.study.bd.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * 使用集合创建RDD
 * @author lx
 * @date 2024/05/04
 */
public class CreateRddByArrayJava {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("CreateRddByArrayJava")
                .setMaster("local");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        // 创建集合 累加集合中的数据
        List<Integer> arr = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> rdd = javaSparkContext.parallelize(arr);
        // reduce阶段计算
        Integer sum = rdd.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println(sum);
        javaSparkContext.stop();

    }
}
