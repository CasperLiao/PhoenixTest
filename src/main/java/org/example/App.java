package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.PropertyConfigurator;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.phoenix.jdbc.PhoenixConnection;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
public class App {

	public static void testHBaseConn(String user, String keytabPath, String tableName) {
		Configuration conf = getHadoopConfiguration();
		UserGroupInformation.setConfiguration(conf);
		Connection connection = null;
		try {
			UserGroupInformation.loginUserFromKeytab(user, keytabPath);
			System.out.println("Login user: " + UserGroupInformation.getLoginUser());

			Configuration configuration = HBaseConfiguration.create();
			configuration.addResource(conf);
			connection = ConnectionFactory.createConnection(configuration);
			Table table = connection.getTable(TableName.valueOf(tableName));
			System.out.println("Scan table: " + table.getName());

			Scan scan = new Scan();
			ResultScanner scanner = table.getScanner(scan);
			for (Result result = scanner.next(); (result != null); result = scanner.next()) {
				System.out.println("Rowkey:" + Bytes.toString(result.getRow()));
				for (Cell cell : result.listCells()) {
					String qualifier = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
							cell.getQualifierLength());
					String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
					System.out.println("Qualifier : " + qualifier + " : Value : " + value);
				}
				System.out.println("----");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static Configuration getHadoopConfiguration() {
		Configuration conf = new Configuration();
		conf.set("hadoop.security.authentication", "Kerberos");
		conf.set("hbase.security.authentication", "Kerberos");
		conf.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@AMDEV.COM");
		conf.set("hbase.zookeeper.quorum", "devm01.devops.com,devm02.devops.com,devm03.devops.com");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		return conf;
	}

	public static void testPhoenixConn(String user, String keytabPath, String tableName) {
		Configuration conf = getHadoopConfiguration();
		UserGroupInformation.setConfiguration(conf);
		PhoenixConnection connection = null;
		PreparedStatement statement = null;
		ResultSet rs = null;
		try {
			UserGroupInformation.loginUserFromKeytab(user, keytabPath);
			System.out.println("Login user: " + UserGroupInformation.getLoginUser());

//			jdbc:phoenix [ :<zookeeper quorum> [ :<port number> [ :<root node> [ :<principal> [ :<keytab file> ] ] ] ] ] 
			String url = "jdbc:phoenix:devm01.devops.com,devm03.devops.com,devm02.devops.com:/hbase:hbase/_HOST@AMDEV.COM:"
					+ keytabPath;
			connection = DriverManager.getConnection(url).unwrap(PhoenixConnection.class);

//			statement = connection.prepareStatement("select DISTINCT(\"TABLE_NAME\") from SYSTEM.CATALOG");
//			rs = statement.executeQuery();
//			while (rs.next()) {
//				System.out.println(rs.getString(1));
//			}
			// If you created the table in HBase using lowercase letters, you need to
			// enclose the table name in quotations. Otherwise, Phoenix will convert the
			// table name to uppercase and it will not find your table.
			// https://stackoverflow.com/questions/31725355/phoenix-error-hbase-table-undefined-even-though-it-is-present
			statement = connection.prepareStatement("select * from \"" + tableName + "\"");
			rs = statement.executeQuery();
			while (rs.next()) {
				System.out.println(rs.getString(1) + "=" + rs.getString(2));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (connection != null) {
					connection.close();
				}
				if (statement != null)
					statement.close();
				if (rs != null)
					rs.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {

		if (args.length != 3) {
			throw new IllegalArgumentException(
					"Exactly 3 parameters required ! kerberos user, keytab path and access table name. "
							+ "Example input, user1@AMDEV.COM conf/user1.keytab users_data");
		}

		System.out.println("Test Kerberos HBase -");
		testHBaseConn(args[0], args[1], args[2]);

		System.out.println("");
		System.out.println("Test Phoenix -");
		testPhoenixConn(args[0], args[1], args[2]);
	}

}
