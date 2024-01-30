

将打包好的jar包上传执行
hadoop jar hadoop-study-1.0-SNAPSHOT.jar  com.study.bd.mapreduce.WordCountJob /words.txt /out

<p>
[root@bigdata01 hadoop-3.2.0]# hdfs dfs -ls /out
Found 2 items
-rw-r--r--   2 root supergroup          0 2024-01-30 22:50 /out/_SUCCESS
-rw-r--r--   2 root supergroup         49 2024-01-30 22:50 /out/part-r-00000
<p>
[root@bigdata01 hadoop-3.2.0]# hdfs dfs -cat /out/part-r-00000
Flink   8
HBase   10
Hadoop  6
Hive    9
Spark   3
Storm   5