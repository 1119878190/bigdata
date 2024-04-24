package com.study.bd.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 操作 hbase
 * <p>
 * 表：创建 ，删除
 * 数据： 增删改查
 *
 * @author lx
 * @date 2024/04/23
 */
public class HBaseOperate {

    private static final Logger logger = LoggerFactory.getLogger(HBaseOperate.class);

    public static void main(String[] args) throws IOException {

        Connection conn = getConn();

        // 添加数据
//        put(conn);

        // 查询数据
//        get(conn);

        /**
         *
         * 查询多版本数据
         * 当列的值有多个版本的时候
         *
         * 修改列族info的最大历史版本存储数量
         *  alter 'student',{NAME=>'info',VERSIONS=>3}
         *
         *  然后在执行下面命令，像列族info中的age列中添加几次数据，试下多历史版本数据存储
         *  put 'student','haozi','info:age','19'
         *  put 'student','haozi','info:age','20'
         *
         *
         */
//        getMoreVersion(conn);


        // 修改数据--->同添加数据

        // 删除数据
//        delete(conn);


        // 创建表
//        createTable(conn);

        // 删除表
        deleteTable(conn);

        conn.close();

    }

    /**
     * 删除表
     *
     * @param conn
     * @throws IOException
     */
    private static void deleteTable(Connection conn) throws IOException {
        Admin admin = conn.getAdmin();
        // 先禁用表
        admin.disableTable(TableName.valueOf("test"));
        admin.deleteTable(TableName.valueOf("test"));
        admin.close();
    }

    /**
     * 创建表
     *
     * @param conn
     * @throws IOException
     */
    private static void createTable(Connection conn) throws IOException {
        // 获取管理员权限，负责对HBase中的表进行操作（DDL操作）
        Admin admin = conn.getAdmin();
        // 创建表
        ColumnFamilyDescriptor infoFamily = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info"))
                // 在这里可以给列族设置一些属性
                .setMaxVersions(3) // 指定最懂存储多少个历史数据版本
                .build();
        ColumnFamilyDescriptor levelFamily = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("level"))
                // 在这里可以给列族设置一些属性
                .setMaxVersions(2)
                .build();

        ArrayList<ColumnFamilyDescriptor> familyList = new ArrayList<>();
        familyList.add(infoFamily);
        familyList.add(levelFamily);
        // 获取TableDescriptor对象
        TableDescriptor desc = TableDescriptorBuilder.newBuilder(TableName.valueOf("test"))// 指定表名
                .setColumnFamilies(familyList)// 指定列族
                .build();
        admin.createTable(desc);
        admin.close();
    }

    private static void delete(Connection conn) throws IOException {
        // 删除数据
        Table table = conn.getTable(TableName.valueOf("student"));
        // 指定rowkey，返回delete对象
        Delete delete = new Delete(Bytes.toBytes("haozi"));
        // [可选] 可以在这里制定要删除rowkey数据哪些列族中的列 如果列有多个版本 只会删除最新的版本 旧版本还在
        //delete.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age"));

        table.delete(delete);
        table.close();
    }

    /**
     * 查询列的多版本数据
     *
     * @param conn
     * @throws IOException
     */
    private static void getMoreVersion(Connection conn) throws IOException {
        Table table = conn.getTable(TableName.valueOf("student"));
        // 指定rowkey
        Get get = new Get(Bytes.toBytes("haozi"));
        // 读取cell中的所有历史版本数据，不设置此配置的时候默认只读取最新版本的数据
        // 可通过 get.readVersions(2) 来指定来获取多少个历史版本数据
        get.readAllVersions();


        Result result = table.get(get);
        // 获取指定列族中的指定列的所有历史版本数据  前提是要设置 get.readAllVersions() 或者 get.readVersions(2) 否则只会获取最新的数据
        List<Cell> columnCells = result.getColumnCells(Bytes.toBytes("info"), Bytes.toBytes("age"));
        for (Cell columnCell : columnCells) {

            long timestamp = columnCell.getTimestamp();
            byte[] value = CellUtil.cloneValue(columnCell);
            System.out.println("值为：" + new String(value) + ",时间戳:" + timestamp);
        }
        table.close();
    }

    private static void get(Connection conn) throws IOException {
        Table table = conn.getTable(TableName.valueOf("student"));
        // 指定rowkey
        Get get = new Get(Bytes.toBytes("haozi"));
        // 【get.addColumn 可选】可以在这里指定要查询该rowkey中的哪些列族中的列
        // 如果不指定，默认查询指定rowkey所有列的内容
//        get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"));
//        get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sex"));

        Result result = table.get(get);
        // 如果不清楚hbase中到底有哪些列族和列，可以使用listCells()获取所有cell单元格，cell对应的是某一列的数据
        List<Cell> cells = result.listCells();
        for (Cell cell : cells) {

            byte[] family = CellUtil.cloneFamily(cell);
            byte[] qualifier = CellUtil.cloneQualifier(cell);
            byte[] value = CellUtil.cloneValue(cell);
            System.out.println("列族:" + new String(family) + ",列：" + new String(qualifier) + ",值:" + new String(value));

        }

        // 如果明确知道hbaes中有哪些列族和列，可以使用getValue(family,qualifier)直接获取指定列族中的指定列的值
        byte[] value = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age"));
        System.out.println("直接获取age的值" + new String(value));

        table.close();
    }

    /**
     * 添加数据
     *
     * @param conn
     * @throws IOException
     */
    private static void put(Connection conn) throws IOException {
        // 获取 table ，指定要操作的表明，表需要提前创建好
        Table table = conn.getTable(TableName.valueOf("student"));
        // 指定 rowkey，返回put对象
        Put put = new Put(Bytes.toBytes("haozi"));
        // 向put对象中指定列族，列，值
        // put 'student','haozi','info:age','18'
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("18"));
        // put 'student','haozi','info:sex','man'
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sex"), Bytes.toBytes("man"));
        // put 'student','haozi','level:class','A'
        put.addColumn(Bytes.toBytes("level"), Bytes.toBytes("class"), Bytes.toBytes("A"));
        table.put(put);
        table.close();
    }

    private static Connection getConn() throws IOException {
        // 获取配置
        Configuration configuration = HBaseConfiguration.create();
        // 指定hbase使用的zk地址，多个用逗号隔开 (可以到hbase-site.xml中查看)
//        configuration.set("hbase.zookeeper.quorum","cdh03:2181,cdh01:2181");
        configuration.set("hbase.zookeeper.quorum", "cdh03:2181");
        // 指定hase再hdfs上的根目录 （可以到hbase-site.xml中查看）
        configuration.set("hbase.rootdir", "hdfs://cdh03:8020/hbase");
        // 创建hbase连接
        return ConnectionFactory.createConnection(configuration);
    }
}
