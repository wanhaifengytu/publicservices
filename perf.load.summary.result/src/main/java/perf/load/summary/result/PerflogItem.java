package perf.load.summary.result;

//https://stackoverflow.com/questions/15303110/jackson-json-field-mapping-capitalization
import com.fasterxml.jackson.annotation.JsonProperty;

public class PerflogItem {
	//CMID CPU EID FRE FWR GID MEM NRE NWR PID RPS RQT SCPU SID SQLC  SQLT UCPU UID URL host.name logdate testiter testreqname testscriptname teststep  
	private String GID;
	private String EID;
	private String RQT;
	private String URL;
	private String logdate;
	
	private String testiter;
	private String testreqname;
	private String testscriptname;
	
	private String teststep;
	
	private Host host;
	
	public Host getHost() {
		return host;
	}

	public void setHost(Host host) {
		this.host = host;
	}

	public String getGID() {
		return GID;
	}
	
	@JsonProperty("GID")
	public void setGID(String gID) {
		GID = gID;
	}
	public String getEID() {
		return EID;
	}
	
	@JsonProperty("EID")
	public void setEID(String eID) {
		EID = eID;
	}
	public String getRQT() {
		return RQT;
	}
	
	@JsonProperty("RQT")
	public void setRQT(String rQT) {
		RQT = rQT;
	}
	public String getURL() {
		return URL;
	}
	
	@JsonProperty("URL")
	public void setURL(String uRL) {
		URL = uRL;
	}
	public String getLogdate() {
		return logdate;
	}
	public void setLogdate(String logdate) {
		this.logdate = logdate;
	}
	public String getTestiter() {
		return testiter;
	}
	public void setTestiter(String testiter) {
		this.testiter = testiter;
	}
	public String getTestreqname() {
		return testreqname;
	}
	public void setTestreqname(String testreqname) {
		this.testreqname = testreqname;
	}
	public String getTestscriptname() {
		return testscriptname;
	}
	public void setTestscriptname(String testscriptname) {
		this.testscriptname = testscriptname;
	}
	public String getTeststep() {
		return teststep;
	}
	public void setTeststep(String teststep) {
		this.teststep = teststep;
	}

	
	
	
}
