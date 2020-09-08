package com.sap.hadoop.client.spark.perfload.logs;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.sap.hadoop.client.spark.CommonUtil;

public class TestSimpleFunctions {
	
	public static void main(String[] args) throws Exception {
		String remainingLine = "PLV=REQ CIP=10.116.30.6 EID=\"EVENT-PLT-LOGINPAGE-lnF7334218ec-20190718084822-0008\" AGN=\"[Mozilla/4.0 (compatible; MSIE 6.0; Windows NT)]\" RID=REQ-[8]  MTD=GET URL=\"/login\" RQT=24 MID=-  PID=- PQ=- SUB=- MEM=2150 CPU=20 UCPU=20 SCPU=0 FRE=3 FWR=0 NRE=0 NWR=0 SQLC=0 SQLT=0 RPS=200 SID=62160BC4173BBC45A327020B6B******.mo-8445329fd GID=0fe6e7c5bbb915050dddf227f08b086e WDF=1234";
		List<String> matchedGID = captureMatchedStringList(remainingLine, "([A-Z]+)=(.+?)\\s+", -1);
		
		for (String gid: matchedGID) {
			System.out.println(gid);;
		}
		
		//Format 18/Jul/2019:09:09:48 +0000 to be Date format
		String sDate1="18/Jul/2019:09:09:48 +0000";
		Date date1=new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss +0000").parse(sDate1);
		
		System.out.println("date " + date1);
		
		System.out.println(generalizeURL("GET /sf/liveprofile?selected_user=US-10349234912&_s.crb=Xysyc2517TnJVFradUhftlnxUWE%3d HTTP/1.1"));
		System.out.println(generalizeURL("GET /login?company=SfPerfWalmart&bplte_logout=1&username=--103593&_s.crb=3L98gcEEYah76615p757AHQkDGM%253d HTTP/1.1 "));
		System.out.println(generalizeURL("GET /xi/ui/edu/pages/profile.xhtml?selected_user=US-100036543&_s.crb=9HkriXANTzyKYSgwgTLKHHpscOM%3d&p_selected_view=scorecard10 HTTP"));
		System.out.println(generalizeURL("GET /xi/ui/edu/pages/profile.xhtml?selected_user=US-100396625&p_selected_view=scorecard10&_s.crb=Xysyc2517TnJVFradUhftlnxUWE%3d HTTP/1.1"));
	}
	
	public static List<String> captureMatchedStringList(String searchStr, String regExp, int groupNum) {
		Pattern pattern = Pattern.compile(regExp);
		Matcher matcher = pattern.matcher(searchStr);
		List<String> strList = new ArrayList<String>();

		while (matcher.find()) {
			if (groupNum > 0) {
				strList.add(matcher.group(groupNum));
			} else {
				strList.add(matcher.group());
			}
		}

		return strList;
	}
	
	// GET /sf/liveprofile?selected_user=US-103492349&_s.crb=Xysyc2517TnJVFradUhftlnxUWE%3d HTTP/1.1
	// GET /login?company=SfPerfWalmart&bplte_logout=1&username=--103593&_s.crb=3L98gcEEYah76615p757AHQkDGM%253d HTTP/1.1 
	/**
	 * Trim the user and scrb information
	 * @param requestURL
	 * @return
	 */
	public static String generalizeURL(String requestURLPart) {
		List<String> matchedURL = CommonUtil.captureMatchedStringList(requestURLPart, "([A-Z]+)\\s+([^\\s]+?)\\s+([^\\s]+)", 2);
		String returnStr = requestURLPart;
		if (matchedURL.size() > 0) {
			returnStr =  matchedURL.get(0);
			List<String> matchedScrb = CommonUtil.captureMatchedStringList(returnStr, "([^\\s]+)crb=([^\\s]+)", 2);
			if (matchedScrb.size() > 0) {
				returnStr = returnStr.replaceAll(matchedScrb.get(0), "");
			}
			
			List<String> matchedUserInfo = CommonUtil.captureMatchedStringList(returnStr, "([^\\s]+)user([^\\s^=]*)=([^\\s^\\&]+)", 3);
			if (matchedUserInfo.size() > 0) {
				System.out.println(" Found match " + matchedUserInfo.get(0));
				returnStr = returnStr.replaceAll(matchedUserInfo.get(0), "");
			}
		}
		
		return returnStr;
	}
	

}
