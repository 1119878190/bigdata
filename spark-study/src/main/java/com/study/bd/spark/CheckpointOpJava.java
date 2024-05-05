package com.study.bd.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author lx
 * @date 2024/05/05
 */
public class CheckpointOpJava {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("CheckpointOpJava")
                .setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // 1.设置checkpoint目录
        sparkContext.setCheckpointDir("hdfs://cdh03:8020/chk001");


        JavaRDD<String> dataRdd = sparkContext.textFile("hdfs://cdh03:8020/tmp/words.txt")
                .persist(StorageLevel.DISK_ONLY());// 执行持久化

        // 2.对rdd执行checkpoint操作
        dataRdd.checkpoint();

        dataRdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s,1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }).saveAsTextFile("hdfs://cdh03:8020/tmp/out");


        sparkContext.stop();
    }
}
