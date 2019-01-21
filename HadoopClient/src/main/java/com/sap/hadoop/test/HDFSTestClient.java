package com.sap.hadoop.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class HDFSTestClient {

	static Configuration conf = new Configuration();

	public static void main(String[] args) throws Exception {
		System.out.println("This is the start of the main HDFS Test Client");
		conf.set("fs.defaultFS", "hdfs://localhost:9000");
		
		HDFSTestClient client = new HDFSTestClient();
		//client.upload("/tmp", "C:\\tools\\apache-maven-3.3.3\\conf");
		//client.mkdir("/tmp/superFolder");
		//client.mkdir("/streaming_checkpoint");
		client.listFiles("/streaming_checkpoint");
		
	}
	
	/**
	 * 
	 * @param remotePath
	 * @param localPath
	 * @throws Exception
	 */
	public void upload(String remotePath, String localPath) throws Exception {
		
		FileSystem fs = FileSystem.get(conf);
		
		Path src = new Path(localPath);
		Path dest = new Path(remotePath);
		
		fs.copyFromLocalFile(src, dest);
		fs.close();
	}

	
	public void copyToLocal(String remotePath,String localPath) throws Exception {
		FileSystem fs = FileSystem.newInstance(conf);
		Path src = new Path(remotePath);
		Path dest = new Path(localPath);
		fs.copyToLocalFile(src, dest);
		fs.close();
		
	}

	public void removeFile(String remotePath) throws Exception {
		FileSystem fs = FileSystem.newInstance(conf);
		Path path = new Path(remotePath);
		//True means recursive
		fs.delete(path, true);
		fs.close();
	}

	public void mkdir(String remotePath) throws Exception {
		FileSystem fs = FileSystem.newInstance(conf);
		fs.mkdirs(new Path(remotePath));
		fs.close();
		
	}

	public void listFiles(String remotePath) throws Exception {
		Path path = new Path(remotePath);
		FileSystem fs = FileSystem.newInstance(conf);
		// True: Recursive Search
		RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(path, true);
		while (iterator.hasNext()) {
			LocatedFileStatus next = iterator.next();
			System.out.println(next.getPath());
		}
		System.out.println("----------------------------------------------------------");
		FileStatus[] fileStatuses = fs.listStatus(path);
		for (int i = 0; i < fileStatuses.length; i++) {
			FileStatus fileStatus = fileStatuses[i];
			System.out.println(fileStatus.getPath());
		}
		fs.close();
	}
}
