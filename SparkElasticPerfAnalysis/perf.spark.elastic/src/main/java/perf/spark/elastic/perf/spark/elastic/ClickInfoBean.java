package perf.spark.elastic.perf.spark.elastic;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ClickInfoBean {
	// "_time",CMID,UID,SVT,EET,MID,PID,PQ
	private Date clickTime;
	
	private String cmid;
	
	private String uid;
	
	private int svt;
	
	private int eet;
	
	private String mid;
	
	private String pid;
	
	private String pq;
	
	private long stayTime;
	
	DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS Z");

	public Date getClickTime() {
		return clickTime;
	}

	public void setClickTime(Date clickTime) {
		this.clickTime = clickTime;
	}

	public String getCmid() {
		return cmid;
	}

	public void setCmid(String cmid) {
		this.cmid = cmid;
	}

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public long getSvt() {
		return svt;
	}

	public void setSvt(int svt) {
		this.svt = svt;
	}

	public long getEet() {
		return eet;
	}

	public void setEet(int eet) {
		this.eet = eet;
	}

	public String getMid() {
		return mid;
	}

	public void setMid(String mid) {
		this.mid = mid;
	}

	public String getPid() {
		return pid;
	}

	public void setPid(String pid) {
		this.pid = pid;
	}

	public String getPq() {
		return pq;
	}

	public void setPq(String pq) {
		this.pq = pq;
	}

	public long getStayTime() {
		return stayTime;
	}

	public void setStayTime(long stayTime) {
		this.stayTime = stayTime;
	}

	@Override
	public String toString() {
		
		return "clickTime=" + df.format(clickTime) + ",cmid=" + cmid + ",uid=" + uid + ",svt=" + svt + ",eet="
				+ eet + ",mid=" + mid + ",pid=" + pid + ",pq=" + pq + ",stayTime=" + stayTime;
	}
	
}
