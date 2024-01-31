package com.study.bd.smallFile;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.IOException;

/**
 * 小文件解决方案值SequenceFile
 *
 * @author lx
 * @date 2024/01/31
 */
public class SequenceFileTest {


    public static void main(String[] args) {
        try {
            // 写文件
//            write("D:\\tmp\\testData", "/seqFile");

            // 读文件
            read("/seqFile");

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 写文件
     *
     * @param inputFilePath  输入目录-windows目录
     * @param outputFilePath 输出文件-hdfs
     */
    public static void write(String inputFilePath, String outputFilePath) throws IOException {
        // 创建一个配置对象
        Configuration configuration = new Configuration();
        // 指定HDFS的地址
        configuration.set("fs.defaultFS", "hdfs://bigdata01:9000");

        // 为了测试方便  先删除hdfs上的文件
//        FileSystem fileSystem = FileSystem.get(configuration);
//        fileSystem.delete(new Path(outputFilePath), true);


        // 构造 opts 数组，有三个元素
        // 第一个：输出路径  第二个 ke的类型  第三个 value的类型
        SequenceFile.Writer.Option[] opts = new SequenceFile.Writer.Option[]{
                SequenceFile.Writer.file(new Path(outputFilePath)),
                SequenceFile.Writer.keyClass(Text.class),
                SequenceFile.Writer.valueClass(Text.class)
        };
        // 创建一个writer实例
        SequenceFile.Writer writer = SequenceFile.createWriter(configuration, opts);

        // 指定需要解压的文件目录
        File inputDirPath = new File(inputFilePath);
        if (inputDirPath.isDirectory()) {
            File[] files = inputDirPath.listFiles();
            for (File file : files) {
                String str = FileUtils.readFileToString(file, "UTF-8");
                String name = file.getName();
                // 像SequenceFile中写入
                writer.append(new Text(name), new Text(str));
            }

        }
        writer.close();

    }


    public static void read(String seqFilePath) throws IOException {

        // 创建一个配置对象
        Configuration configuration = new Configuration();
        // 指定HDFS的地址
        configuration.set("fs.defaultFS", "hdfs://bigdata01:9000");
        // 创建阅读器
        SequenceFile.Reader reader = new SequenceFile.Reader(configuration, SequenceFile.Reader.file(new Path(seqFilePath)));

        Text key = new Text();
        Text value = new Text();

        while (reader.next(key, value)) {

            System.out.println("文件名：" + key.toString() + ",");
            System.out.println("文件内容:" + value.toString());


        }
        reader.close();

    }

}
