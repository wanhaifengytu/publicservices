package perf.load.summary.result;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import kafka.serializer.StringDecoder;

public class SparkStreamingHANADB {

	public static void main(String[] args) throws Exception {
		System.out.println("This is the start of SparkStreaming HANA DB");
		Properties props = CommonUtil.loadProperties("sparkParams.properties");
		String master = (String) props.getProperty("master");
		SparkConf conf = new SparkConf().setMaster(master).setAppName("monsoonperflogsapp");

		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
		
		Map<String, String> kafkaParams = new HashMap<String, String>();
		String kafkaBroker = (String) props.getProperty("kafkaBrokers");
		kafkaParams.put("metadata.broker.list", kafkaBroker);

		String kafkaTopics = "monsoonperflogs";
		String[] kafkaTopicsSplited = kafkaTopics.split(",");

		Set<String> topics = new HashSet<String>();
		for (String kafkaTopic : kafkaTopicsSplited) {
			topics.add(kafkaTopic);
		}

		JavaPairInputDStream<String, String> perfLogDStream = KafkaUtils.createDirectStream(jssc, String.class,
				String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);
		
		JavaDStream<String> mappedStrRDD = perfLogDStream.map(inputMessage-> {
			//System.out.println("This is the streaming received " + inputMessage._2 );
			return inputMessage._2;
		});
  
		//mappedStrRDD.print();
		mappedStrRDD.foreachRDD(rdd-> {
			rdd.foreachPartition(eachItem-> {
				ObjectMapper mapper = new ObjectMapper();
				mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
				
				Connection hanaJDBCConnection = getConnection(props);
				Statement stmt = hanaJDBCConnection.createStatement();

				while(eachItem.hasNext()) {
					PerflogItem perflogItem = mapper.readValue( eachItem.next(), PerflogItem.class);
					//String sqlInsert = "insert into SFQA05_TESTH_E.perflogbase(gid, eid, rqt, url, logdate, testiter, testreqname, hostname, testscriptname, teststep) values (?,?,?,?,?,?,?,?,?, ?);";
					String sqlInsert = "insert into SFQA05_TESTH_E.perflogbase(gid, logdate, eid, rqt, url, testiter, testreqname, hostname, testscriptname,teststep) values ('";
					sqlInsert = sqlInsert + perflogItem.getGID() + "',";
					String logdate = perflogItem.getLogdate();
					if (logdate!=null) {
						logdate = logdate.substring(0, logdate.indexOf(","));
					}
					sqlInsert = sqlInsert + " TO_TIMESTAMP ('" + logdate  +"', 'YYYY-MM-DD HH24:MI:SS'),";
					
					sqlInsert = sqlInsert + "'" +  perflogItem.getEID() + "',";
					String rqt = perflogItem.getRQT();
					if (rqt == null) {
						rqt = "0";
					}
					sqlInsert = sqlInsert + rqt + ",";
					
					sqlInsert = sqlInsert + "'" +  perflogItem.getURL() + "',";
					sqlInsert = sqlInsert +   perflogItem.getTestiter() + ",";
					sqlInsert = sqlInsert + "'" +  perflogItem.getTestreqname() + "',";
					sqlInsert = sqlInsert + "'" +  perflogItem.getHost().getName() + "',";
					sqlInsert = sqlInsert + "'" +  perflogItem.getTestscriptname() + "',";
					sqlInsert = sqlInsert + "'" +  perflogItem.getTeststep() + "'";
					
					sqlInsert = sqlInsert + ");";

					int rows = stmt.executeUpdate(sqlInsert);
					//System.out.println("sql statement rows " + rows + " --- " + sqlInsert );
				}
				if (stmt!=null) { 
					stmt.close();
				}
				hanaJDBCConnection.close();
			});
		   });
		
		jssc.start();
		jssc.awaitTermination();
	}
	
	public static Connection getConnection(Properties props) {
		String hanajdbc = (String) props.getProperty("hanajdbc");
		String hanauser = (String) props.getProperty("hanauser");
		String hanapassword = (String) props.getProperty("hanapassword");
		
	      Connection connection = null;
	      try {                  
	         connection = DriverManager.getConnection(hanajdbc,hanauser,hanapassword);                  
	      } catch (SQLException e) {
	         System.err.println("Connection Failed. User/Passwd Error");
	      }
	      return connection;
	}
}