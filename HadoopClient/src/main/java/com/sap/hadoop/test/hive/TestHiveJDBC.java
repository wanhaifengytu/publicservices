package com.sap.hadoop.test.hive;

import java.sql.*;

//Issues resolved 
//https://blog.csdn.net/u010379814/article/details/60755846
//https://blog.csdn.net/GK_kk/article/details/68924943 

public class TestHiveJDBC {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	private static String url = "jdbc:hive2://localhost:9999/hive";
	private static String user = "hadoop";
	private static String password = "";

	private static Connection conn = null;
	private static Statement stmt = null;
	private static ResultSet rs = null;

	public static void main(String[] args) throws Exception {
		System.out.println("This is the start of HiveJDBC Test Program");

		Class.forName(driverName);
		conn = DriverManager.getConnection(url, user, password);
		stmt = conn.createStatement();

		showDatabases();
		
		destory();
	}

	public static void createDatabase() throws Exception {
		String sql = "create database hive_jdbc_test";
		System.out.println("Running: " + sql);
		stmt.execute(sql);
	}

	//Show databases;
	public static void showDatabases() throws Exception {
		String sql = "show databases";
		System.out.println("Running: " + sql);
		rs = stmt.executeQuery(sql);
		while (rs.next()) {
			System.out.println(rs.getString(1));
		}
	}

	public static void createTable() throws Exception {
		String sql = "create table emp(\n" + "empno int,\n" + "ename string,\n" + "job string,\n" + "mgr int,\n"
				+ "hiredate string,\n" + "sal double,\n" + "comm double,\n" + "deptno int\n" + ")\n"
				+ "row format delimited fields terminated by '\\t'";
		System.out.println("Running: " + sql);
		stmt.execute(sql);
	}

	//Query all tables 
	public static void showTables() throws Exception {
		String sql = "show tables";
		System.out.println("Running: " + sql);
		rs = stmt.executeQuery(sql);
		while (rs.next()) {
			System.out.println(rs.getString(1));
		}
	}

	//Check table description
	public static void descTable() throws Exception {
		String sql = "desc emp";
		System.out.println("Running: " + sql);
		rs = stmt.executeQuery(sql);
		while (rs.next()) {
			System.out.println(rs.getString(1) + "\t" + rs.getString(2));
		}
	}

	public static void destory() throws Exception {
		if (rs != null) {
			rs.close();
		}
		if (stmt != null) {
			stmt.close();
		}
		if (conn != null) {
			conn.close();
		}
	}
}
