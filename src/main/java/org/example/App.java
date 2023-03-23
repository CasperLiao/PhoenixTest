package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.PropertyConfigurator;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.phoenix.jdbc.PhoenixConnection;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;

import org.apache.hadoop.hbase.util.Threads;
public class App {

    public Connection getConn(String url) {
        System.setProperty("java.security.krb5.conf", "/home/user1/krb5.conf");
        // 连接hadoop环境，进行 Kerberos认证
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");
        // linux 环境会默认读取/etc/nokrb.cnf文件，win不指定会默认读取C:/Windows/krb5.ini
        /*if (System.getProperty("os.name").toLowerCase().startsWith("win")) {
            System.setProperty("java.security.krb5.conf", "/conf/krb5.conf");
        }*/
        conf.set("hadoop.security.authentication", "Kerberos");
        conf.set("hbase.security.authentication", "Kerberos");
        conf.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@AMDEV.COM");
        conf.set("hbase.zookeeper.quorum", "devm01.devops.com,devm03.devops.com,devm02.devops.com");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        UserGroupInformation.setConfiguration(conf);
        Connection conn = null;
        try {
//            UserGroupInformation.loginUserFromKeytab("user1", "/home/user1/user1.keytab");
           Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
//            Class.forName("org.apache.phoenix.queryserver.client.Driver");
            // kerberos环境下Phoenix的jdbc字符串为 jdbc:phoenix:zk:2181:/znode:principal:keytab
//            String url = "jdbc:phoenix:192.168.30.121:user1:user1/user1@AMDEV.COM:/home/user1/user1.keytab";
            System.out.println("Connecting .....");
//            String url = "jdbc:phoenix:thin:url=http://localhost:8765;serialization=PROTOBUF; authentication=SPENGO;principal=user1@AMDEV.COM;keytab=/home/user1/user1.keytab";
//            String url = "jdbc:phoenix:thin:url=http://192.168.30.121:8765;serialization=PROTOBUF;";
            Properties prop = new Properties();
            prop.put("hadoop.security.authentication", "Kerberos");
            prop.put("hbase.security.authentication", "Kerberos");
            prop.put("hbase.regionserver.kerberos.principal", "hbase/_HOST@AMDEV.COM");
            prop.put("hbase.zookeeper.quorum", "devm01.devops.com,devm03.devops.com,devm02.devops.com");
            prop.put("hbase.zookeeper.property.clientPort", "2181");
            prop.put("hbase.myclient.keytab", "/home/user1/user1.keytab");
            prop.put("hbase.myclient.principal", "user1");
            prop.put("phoenix.schema.isNamespaceMappingEnabled", "true");
            conn = DriverManager.getConnection(url, prop);
        } catch (SQLException e1) {
            e1.printStackTrace();
        } catch(Exception e) {
            e.printStackTrace();
        }
        return conn;

    }

    /**
     * 对表执行操作
     * 通过phoenix 创建表、插入数据、索引、查询数据
     */
    public void operTable( String jdbcurl) {
        Connection conn = getConn(jdbcurl);
        ResultSet rs = null;
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.execute("DROP TABLE if EXISTS testjdbc");
            stmt.execute("CREATE TABLE testjdbc (id INTEGER NOT NULL PRIMARY KEY, content VARCHAR)");
            // 创建二级索引
//            stmt.execute("create index test_idx on testjdbc(content)");
            // 循环插入数据
            for (int i = 1; i <= 100; i++) {
                stmt.executeUpdate("upsert INTO testjdbc VALUES (" + i + ",'The num is " + i + "')");
            }
            conn.commit();
            PreparedStatement statement = conn.prepareStatement("SELECT * FROM testjdbc limit 10");
            rs = statement.executeQuery();
            while (rs.next()) {
//                log.info("-------------The num is ---------------" + rs.getInt(1));
                String id = rs.getString("id");
                String content = rs.getString("content");
                System.out.println("id = " + id + "; " + "content = " + content);
            }
        } catch (SQLException e) {
//            log.error(e.getMessage());
            e.printStackTrace();
        } finally {
            closeRes(conn, stmt, rs);
        }
    }

    /**
     * 关闭资源
     *
     * @param conn
     * @param statement
     * @param rs
     */
    public void closeRes(Connection conn, Statement statement, ResultSet rs) {
        try {
            if (conn != null) {
                conn.close();
            }
            if (statement != null)
                statement.close();
            if (rs != null)
                rs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        App test = new App();
        String log4jConfPath = "/home/user1/log4j.properties";
        PropertyConfigurator.configure(log4jConfPath);
        test.operTable(args[0]);
    }

}
