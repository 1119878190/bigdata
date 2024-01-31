package com.study.bd.smallFile;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * 小文件解决方案之MapFile
 * <p>
 * MapFile就是有序的SequenceFile
 *
 * @author lx
 * @date 2024/01/31
 */
public class FileMap {


    public static void main(String[] args) throws IOException {
//        write("D:\\tmp\\testData", "/mapFile");
        read("/mapFile");
    }

    /**
     * 写文件
     *
     * @param inputFilePath 输入目录-windows目录
     * @param outputDir     输出目录-hdfs
     */
    public static void write(String inputFilePath, String outputDir) throws IOException {
        // 创建一个配置对象
        Configuration configuration = new Configuration();
        // 指定HDFS的地址
        configuration.set("fs.defaultFS", "hdfs://bigdata01:9000");

        // 为了测试方便  先删除hdfs上的文件
//        FileSystem fileSystem = FileSystem.get(configuration);
//        fileSystem.delete(new Path(outputFilePath), true);


        // 构造 opts 数组，有两个元素
        //  第一个 ke的类型  第二个 value的类型
        SequenceFile.Writer.Option[] opts = new SequenceFile.Writer.Option[]{
                MapFile.Writer.keyClass(Text.class),
                MapFile.Writer.valueClass(Text.class)
        };

        // 创建一个Writer实例
        MapFile.Writer writer = new MapFile.Writer(configuration, new Path(outputDir), opts);

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


    /**
     * 读文件
     *
     * @param inputDir mapFile文件夹路径
     * @throws IOException
     */
    private static void read(String inputDir) throws IOException {

        // 创建一个配置对象
        Configuration configuration = new Configuration();
        // 指定HDFS的地址
        configuration.set("fs.defaultFS", "hdfs://bigdata01:9000");
        // 创建阅读器
        MapFile.Reader reader = new MapFile.Reader(new Path(inputDir), configuration);

        Text key = new Text();
        Text value = new Text();

        while (reader.next(key, value)) {

            System.out.println("文件名：" + key.toString() + ",");
            System.out.println("文件内容:" + value.toString());


        }
        reader.close();

    }

}
