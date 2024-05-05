package com.study.bd.spark;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * 需求:计算每个大区当天金币收入TopN的主播
 * <p>
 * video_info.log 主播的开播记录，其中包含主播的id:uid、直播间id:vid、大区:area、视频开播时长:length、增加粉丝数量:follow等信息
 * gift_record.log 用户送礼记录，其中包含送礼,人id:uid，直播间id:vid，礼物id:good_id，金币数gold_sum
 *
 * @author lx
 * @date 2024/05/04
 */
public class TopNJava {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("TopNJava")
                .setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // 1.首先获取两份数据中的核心字段，使用fastjson包解析数据
        JavaRDD<String> giftRecordRDD = sparkContext.textFile("D:\\developCode\\bigdata\\spark-study\\src\\main\\resources\\gift_record.log");
        JavaRDD<String> videoRecordRDD = sparkContext.textFile("D:\\developCode\\bigdata\\spark-study\\src\\main\\resources\\video_info.log");

        //(vid,(uid,area))
        JavaPairRDD<String, Tuple2<String, String>> videoInfoFieldRDD = videoRecordRDD.mapToPair(new PairFunction<String, String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, Tuple2<String, String>> call(String s) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(s);
                String vid = jsonObject.getString("vid");
                String uid = jsonObject.getString("uid");
                String area = jsonObject.getString("area");
                return new Tuple2<>(vid, new Tuple2<>(uid, area));

            }
        });

        // (vid,gold)
        JavaPairRDD<String, Integer> giftRecordFieldRDD = giftRecordRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(s);
                String vid = jsonObject.getString("vid");
                Integer god = Integer.parseInt(jsonObject.getString("gold"));
                return new Tuple2<>(vid, god);

            }
        });

        // 2.对用户送礼记录数据进行聚合，对相同vid的数据求和
        // (vid,gold_sum)
        JavaPairRDD<String, Integer> giftReduceFieldAggRDD = giftRecordFieldRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        // 3.把两份数据join到一块，vid作为join的key
        // (vid,((uuid,area),gold_sum))
        JavaPairRDD<String, Tuple2<Tuple2<String, String>, Integer>> joinRDD = videoInfoFieldRDD.join(giftReduceFieldAggRDD);


        // 4.使用map迭代join之后的数据，最后获取到uid，area，gold_sum
        //((udi,area),good_sum)
        JavaPairRDD<Tuple2<String, String>, Integer> joinMapRDD = joinRDD.mapToPair(new PairFunction<Tuple2<String, Tuple2<Tuple2<String, String>, Integer>>, Tuple2<String, String>, Integer>() {
            @Override
            public Tuple2<Tuple2<String, String>, Integer> call(Tuple2<String, Tuple2<Tuple2<String, String>, Integer>> tup) throws Exception {

                // joinRDD  (vid,((uuid,area),gold_sum))
                // 获取uid
                String uuid = tup._2._1._1;
                // 获取area
                String area = tup._2._1._2;
                // 获取gold_sum
                Integer gold_sum = tup._2._2;
                return new Tuple2<>(new Tuple2<>(uuid, area), gold_sum);
            }
        });

        // 5.使用reduceByKey算子对数据进行聚合
        // 输出 ((uid,area),gold_sum_all)
        JavaPairRDD<Tuple2<String, String>, Integer> reduceRDD = joinMapRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        // 6.接下来对需要使用groupByKey对数据进行分组，所以先使用map进行转换 一个主播uid可能有多个开播记录
        // 输出：map  (area,(uid,gold_sum_all))
        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> groupRDD = reduceRDD.mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Integer>, String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Tuple2<String, Integer>> call(Tuple2<Tuple2<String, String>, Integer> tup) throws Exception {
                return new Tuple2<>(tup._1._2, new Tuple2<>(tup._1._1, tup._2));
            }
        }).groupByKey();

        // 7.使用map迭代每个分组内的数据，按照金币数量倒序排序，取前N个，最终输出area，topN
        // (area,topN)
        JavaPairRDD<String, String> top3RDD = groupRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Tuple2<String, Integer>>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, Iterable<Tuple2<String, Integer>>> tup) throws Exception {
                String area = tup._1;
                // 同一个大区内的 数据
                ArrayList<Tuple2<String, Integer>> tupleList = Lists.newArrayList(tup._2);
                // 对集合中的元素进行排序
                Collections.sort(tupleList, new Comparator<Tuple2<String, Integer>>() {
                    @Override
                    public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
                        // 根据gold_sum_all排序
                        return o2._2 - o1._2;
                    }
                });
                StringBuffer sb = new StringBuffer();
                for (int i = 0; i < tupleList.size(); i++) {
                    if (i <= 3) {
                        Tuple2<String, Integer> t = tupleList.get(i);
                        sb.append(t._1 + ":" + t._2);
                        sb.append(",");
                    }
                }
                String result  = sb.substring(0, sb.length() - 1);
                return new Tuple2<>(area, result);
            }
        });

        // 遍历
        top3RDD.foreach(new VoidFunction<Tuple2<String, String>>() {
            @Override
            public void call(Tuple2<String, String> stringStringTuple2) throws Exception {
                System.out.println(stringStringTuple2);
            }
        });


    }
}
