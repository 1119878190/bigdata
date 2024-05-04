package com.study.bd.spark;

import org.apache.commons.lang.time.StopWatch;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *
 * 将RDD数据持久化到内存 cache()
 * @author lx
 * @date 2024/05/04
 */
public class PersistRddJava {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("PersistRddJava")
                .setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        String path = "D:\\tmp\\testData\\5942.csv";

        JavaRDD<String> dataRDD = javaSparkContext.textFile(path).cache();

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        long count = dataRDD.count();
        stopWatch.stop();
        System.out.println("第一次耗时"+stopWatch.getTime() + ",count:"+count);


        StopWatch stopWatch2 = new StopWatch();
        stopWatch2.start();
        long count2 = dataRDD.count();
        stopWatch2.stop();
        System.out.println("第2次耗时"+stopWatch2.getTime() + ",count:"+count2);


    }
}
