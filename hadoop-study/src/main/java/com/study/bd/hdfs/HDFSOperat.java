package com.study.bd.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * java代码操作hdfs
 * <p>
 * 上传，下载，删除文件
 *
 * @author lx
 * @date 2024/01/29
 */
public class HDFSOperat {


    public static void main(String[] args) {


        try {
            Configuration configuration = new Configuration();
            // 对应 core-site.xml配置
            configuration.set("fs.defaultFS", "hdfs://bigdata01:9000");
            // 获取操作HDFS的对象
            FileSystem fileSystem = FileSystem.get(configuration);

            // 上传文件
//            upload(fileSystem);


            // 下载文件
//            download(fileSystem);

            // 删除文件
            // 如果要递归删除目录，第二个参数为true
            delete(fileSystem);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * 删除文件
     *
     * @param fileSystem
     * @throws IOException
     */
    private static void delete(FileSystem fileSystem) throws IOException {
        boolean isDelete = fileSystem.delete(new Path("/ignite-config.xml"), true);
    }

    /**
     * 下载文件
     *
     * @param fileSystem
     * @throws IOException
     */
    private static void download(FileSystem fileSystem) throws IOException {
        FSDataInputStream inputStream = fileSystem.open(new Path("/ignite-config.xml"));
        FileOutputStream fileOutputStream = new FileOutputStream("D:\\ignite-config.xml.new");
        IOUtils.copyBytes(inputStream, fileOutputStream, 1024, true);
    }

    /**
     * 上传文件
     *
     * @param fileSystem
     * @throws IOException
     */
    private static void upload(FileSystem fileSystem) throws IOException {
        // 上传文件
        // 获取本地文件输入流
        FileInputStream inputStream = new FileInputStream("D:\\ignite-config.xml");
        // 获取HDFS输出流
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/ignite-config.xml"));
        // 将文件拷贝
        IOUtils.copyBytes(inputStream, fsDataOutputStream, 1024, true);
    }


}
