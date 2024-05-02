package com.study.bd.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * scan  操作
 *
 * @author lx
 * @date 2024/05/02
 */
public class HBaseScanFilter {

    public static void main(String[] args) throws IOException {

        // 获取配置
        Configuration configuration = HBaseConfiguration.create();
        // 指定hbase使用的zk地址，多个用逗号隔开 (可以到hbase-site.xml中查看)
//        configuration.set("hbase.zookeeper.quorum","cdh03:2181,cdh01:2181");
        configuration.set("hbase.zookeeper.quorum", "cdh03:2181,cdh02:2181,cdh01:2181");
        // 指定hase再hdfs上的根目录 （可以到hbase-site.xml中查看）
        configuration.set("hbase.rootdir", "hdfs://cdh03:8020/hbase");
        // 创建hbase连接
        Connection connection = ConnectionFactory.createConnection(configuration);
        // 获取Table对象， 指定要操作的表名，表需要提前创建好
        Table table = connection.getTable(TableName.valueOf("s1"));

        Scan scan = new Scan();
        // 范围查询： 指定查询区间，提高性能
        // 这是一个左闭右开的区间，也就是查询的结果中包含左边的，不包含右边的
        scan.withStartRow(Bytes.toBytes("a"));
        scan.withStopRow(Bytes.toBytes("f"));

        // 添加Filter对数据进行过滤： 使用rowFilter进行过滤 ，获取rowkey小等于d的数据
        RowFilter filter = new RowFilter(CompareOperator.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("d")));
        scan.setFilter(filter);

        // 获取查询结果
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            // rowkey
            byte[] rowKey = result.getRow();
            for (Cell cell : cells) {
                byte[] family = CellUtil.cloneFamily(cell);
                byte[] qualifier = CellUtil.cloneQualifier(cell);
                byte[] value = CellUtil.cloneValue(cell);
                System.out.println("rowkey:" + new String(rowKey) + "列族:" + new String(family) + ",列：" + new String(qualifier) + ",值:" + new String(value));
            }

        }


        // 关闭连接
        table.close();
        connection.close();
    }
}
