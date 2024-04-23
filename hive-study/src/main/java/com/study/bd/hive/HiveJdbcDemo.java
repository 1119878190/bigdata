package com.study.bd.hive;

import java.sql.*;

/**
 * JDBC代码操作 Hive
 *
 * @author lx
 * @date 2024/02/03
 */
public class HiveJdbcDemo {


    public static void main(String[] args) throws Exception {
        //指定hiveserver2的连接
        String jdbcUrl = "jdbc:hive2://cdh03:10000";
        //获取jdbc连接，这里的user使用root，就是linux中的用户名，password随便指定即可
        Connection conn = DriverManager.getConnection(jdbcUrl, "root", "123456");
        //获取Statement
        Statement stmt = conn.createStatement();
        //指定查询的sql
        String sql = "select * from t1";

        //执行sql
        ResultSet res = stmt.executeQuery(sql);
        //循环读取结果
        while (res.next()) {
            System.out.println(res.getInt("id") + "\t" + res.getString("name"));
        }

    }


}
