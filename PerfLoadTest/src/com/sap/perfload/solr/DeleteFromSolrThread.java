package com.sap.perfload.solr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;

public class DeleteFromSolrThread implements Runnable {

	private String solrUrl;
	private String name;
	private String coreName;
	private Properties props;
	private String idsFileName;

	private static Logger logger = Logger.getLogger(DeleteFromSolrThread.class);

	public DeleteFromSolrThread(String name, Properties props, String solrUrl, String idsFileName, String coreName) {
		super();
		this.coreName = coreName;
		this.solrUrl = solrUrl;
		this.name = name;
		this.props = props;
		this.idsFileName = idsFileName;
	}

	@Override
	public void run() {
		try {
			File file = new File(this.idsFileName);
			logger.debug("Will delete all ids from " + this.idsFileName);
			String urlString = this.solrUrl + this.coreName;
			SolrClient client = new HttpSolrClient.Builder(urlString).build();

			BufferedReader br;
			br = new BufferedReader(new FileReader(file));
			String readLine = "";
			while ((readLine = br.readLine()) != null) {
				String query = "id:" + readLine.trim();
				client.deleteByQuery(query);
				client.commit();
				// logger.debug("delete by query--" + query);
				// sleep for a while
				Thread.sleep(Integer.parseInt(this.props.getProperty("deleteSleep").toString()));
			}
			client.close();
		
			//Finally remove the file because all Ids are removed.
			if (file.exists()) {
				file.delete();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
