package com.study.bd.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * 单词计数-spark案例
 *
 * @author lx
 * @date 2024/05/02
 */
public class WordCountJava {

    public static void main(String[] args) {

        // 第一步：创建JavaSparkContext
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("WordCountJava");
//                .setMaster("local"); // setMaster(local)为本地测试运行，线上运行需要去掉
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        // 第二步：加载数据
        String path = "D:\\tmp\\testData\\words.txt";
        if (args.length == 1) {
            // 脚本指定hdfs文件路径  hdfs://cdh03:8020/tmp/words.txt
            path = args[0];
        }
        JavaRDD<String> linesRDD = sc.textFile(path);
        // 第三步：对数据进行切割
        // 注意：FlatMapFunction的泛型，第一个表示输入数据类型，第二个表示输出数据类型
        JavaRDD<String> wordsRDD = linesRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();

            }
        });
        // 第四步：迭代words，将每个word转换为(word,1)这种形式
        JavaPairRDD<String, Integer> pairRDD = wordsRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });
        // 第五步，根据key(其实就是word)进行分组聚合统计
        JavaPairRDD<String, Integer> wordCountRDD = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        // 第六步将结果打印到控制台
        // 注意：只有当任务执行到这一行代码的时候，任务才会真正的开始执行计算
        // 如果任务中没有这一行代码，前面的所有算子是不会执行的，最少要有一个action算子
        wordCountRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1 + "--" + stringIntegerTuple2._2);
            }
        });
        // 第七步：停驶SparkContext
        sc.stop();


    }

}
