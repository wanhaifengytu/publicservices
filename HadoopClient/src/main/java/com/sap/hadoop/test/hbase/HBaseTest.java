package com.sap.hadoop.test.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTest {
	 private static Configuration conf = null;
	public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		System.out.println("This is the start of Hbase Test");
		
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "127.0.0.1:2181");
		//conf.set("master", "localhost:60010");
		HBaseAdmin client = new HBaseAdmin(conf);
		
		HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("mytableNew"));
		HColumnDescriptor h1 = new HColumnDescriptor("info");
		HColumnDescriptor h2 = new HColumnDescriptor("grade");
		
		htd.addFamily(h1);
		htd.addFamily(h2);
		client.createTable(htd);
		
		String[] column1 = { "title", "content", "tag" };
		String[] value1 = { "Head First HBase",
				"HBase is the Hadoop database. Use it when you need random, realtime read/write access to your Big Data.",
				"Hadoop,HBase,NoSQL" };
		String[] column2 = { "name", "nickname" };
		String[] value2 = { "nicholas", "lee" };
		
		addData("test1" , "mytableNew",column1, value1, column2, value2);
		
		client.close();
		
		
	}

	public static void addData(String rowKey, String tableName, String[] column1, String[] value1, String[] column2,
			String[] value2) throws IOException {
		Put put = new Put(Bytes.toBytes(rowKey));// 设置rowkey
		HTable table = new HTable(conf, Bytes.toBytes(tableName));// HTabel负责跟记录相关的操作如增删改查等//
																	// 获取表
		HColumnDescriptor[] columnFamilies = table.getTableDescriptor() // 获取所有的列族
				.getColumnFamilies();
 
		for (int i = 0; i < columnFamilies.length; i++) {
			String familyName = columnFamilies[i].getNameAsString(); // 获取列族名
			if (familyName.equals("article")) { // article列族put数据
				for (int j = 0; j < column1.length; j++) {
					put.add(Bytes.toBytes(familyName), Bytes.toBytes(column1[j]), Bytes.toBytes(value1[j]));
				}
			}
			if (familyName.equals("author")) { // author列族put数据
				for (int j = 0; j < column2.length; j++) {
					put.add(Bytes.toBytes(familyName), Bytes.toBytes(column2[j]), Bytes.toBytes(value2[j]));
				}
			}
		}
		table.put(put);
		System.out.println("add data Success!");
	}
	
}
