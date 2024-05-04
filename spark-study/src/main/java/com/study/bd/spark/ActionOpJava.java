package com.study.bd.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * action算子操作
 * <p>
 * reduce:聚合计算
 * collect:获取元素集合
 * take(n):获取前n个元素
 * count:获取元素总数
 * saveAsTextFile: 保存文件
 * countByKey: 统计相同的key出现多少次
 * foreach： 迭代便遍历元素
 *
 * @author lx
 * @date 2024/05/04
 */
public class ActionOpJava {

    public static void main(String[] args) {

        JavaSparkContext sparkContext = getSparkContext();


        // reduce:聚合计算
        reduceOp(sparkContext);

        // collect:获取元素集合
        collectOp(sparkContext);

        // take(n):获取前n个元素
        takeOp(sparkContext);

        // count:获取元素总数
        countOp(sparkContext);

        // saveAsTextFile: 保存文件
//        saveAsTextFileOp(sparkContext);

        // countByKey: 统计相同的key出现多少次
        countByKeyOp(sparkContext);

        // foreach： 迭代便遍历元素
        foreachOp(sparkContext);

        sparkContext.stop();

    }

    private static void foreachOp(JavaSparkContext sparkContext) {
        JavaRDD<Integer> dataRDD = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        dataRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });


    }

    private static void countByKeyOp(JavaSparkContext sparkContext) {

        Tuple2<String, Integer> t1 = new Tuple2<>("A", 1001);
        Tuple2<String, Integer> t2 = new Tuple2<>("B", 1002);
        Tuple2<String, Integer> t3 = new Tuple2<>("A", 1003);
        Tuple2<String, Integer> t4 = new Tuple2<>("C", 1004);

        JavaRDD<Tuple2<String, Integer>> dataRDD = sparkContext.parallelize(Arrays.asList(t1, t2, t3, t4));
        // 想要使用countByKey，需要先使用mapToPair对RDD进行转换
        Map<String, Long> res = dataRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Integer> tup) throws Exception {
                return new Tuple2<>(tup._1, tup._2);
            }
        }).countByKey();

        for (Map.Entry<String, Long> entry : res.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue());

        }

    }

    private static void saveAsTextFileOp(JavaSparkContext sparkContext) {
        JavaRDD<Integer> dataRDD = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        dataRDD.saveAsTextFile("hdfs://cdh03:8020/tmp/out002");

    }

    private static void countOp(JavaSparkContext sparkContext) {
        JavaRDD<Integer> dataRDD = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        long count = dataRDD.count();
        System.out.println(count);
    }

    private static void takeOp(JavaSparkContext sparkContext) {
        JavaRDD<Integer> dataRDD = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5));

        List<Integer> take = dataRDD.take(2);
        for (Integer i : take) {
            System.out.println(i);
        }

    }

    private static void collectOp(JavaSparkContext sparkContext) {
        JavaRDD<Integer> dataRDD = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        List<Integer> collect = dataRDD.collect();

        for (Integer i : collect) {
            System.out.println(i);
        }

    }

    private static void reduceOp(JavaSparkContext sparkContext) {

        JavaRDD<Integer> dataRDD = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        Integer num = dataRDD.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println(num);

    }


    private static JavaSparkContext getSparkContext() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("TransformationOpJava")
                .setMaster("local");
        return new JavaSparkContext(sparkConf);
    }
}
