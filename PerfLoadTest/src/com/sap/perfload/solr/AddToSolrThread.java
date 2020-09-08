package com.sap.perfload.solr;

import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;

import com.sap.perfload.util.CommonUtil;

public class AddToSolrThread implements Runnable {
	
	private String solrUrl;
	private String name;
	private String coreName;
	private Properties props;
	private int threadNum =0;

	private static Logger logger = Logger.getLogger(AddToSolrThread.class);

	public AddToSolrThread(String name, Properties props, String solrUrl, int threadNum, String coreName) {
		super();
		this.coreName= coreName;
		this.solrUrl = solrUrl;
		this.name = name;
		this.props = props;
		this.threadNum = threadNum;
	}

	@Override
	public void run() {
		try {
			int addBatchSize = Integer.parseInt(this.props.getProperty("addBatchSize").toString());

			logger.debug("start to add to solr with batch size " + addBatchSize);
			String urlString = this.solrUrl + this.coreName;
			String idStrFile = this.props.getProperty("idFiles").toString()+ "_" + this.coreName;
			SolrClient client = new HttpSolrClient.Builder(urlString).build();
			for (int i = 0; i < addBatchSize; i++) {
				String id = CommonUtil.generateUniqueKey();
				
				SolrInputDocument document = new SolrInputDocument();
				document.setField("id", id);
				document.setField("phone", CommonUtil.generateRandomString(null, 20, null));
				document.setField("city", CommonUtil.generateRandomString(null, 20, null));
				document.setField("first_name", "patric wan insrhldcumf");
				document.setField("last_name", "wanetaoinsrhldcumfp"+i);

				client.add(document);
				CommonUtil.writeLineToFile(idStrFile + "_" + this.threadNum + ".txt", id);
			}

			client.commit();
			client.close();
		} catch (SolrServerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
