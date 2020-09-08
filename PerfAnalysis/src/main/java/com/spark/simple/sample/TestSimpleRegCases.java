package com.spark.simple.sample;

import java.util.List;

import com.spark.analysis.logs.CommonUtil;

public class TestSimpleRegCases {

	public static void main(String[] args) {
		System.out.println("This is the start of simple reg cases");
		
		String requestURLPart = "/login?company=SfPerfWalmart&bplte_logout=1&username=pahmed7&_s.crb=JNrzuoG9TOjHwFgIZsAk4x7VmWU%253d";
		// /sf/start?_s.crb=RPeIfjOqpZrYcjsYIV3zyps0GBs%253d
		///login?company=SfPerfWalmart&bplte_logout=1&username=pahmed7&_s.crb=E8zHsitosvh9d%252fbvUFREHo2Mg%252fs%253d
		//System.out.println("after " + CommonUtil.generalizeURL(simpleReg));
		
		List<String> matchedURL = CommonUtil.captureMatchedStringList(requestURLPart, "([^\\s]+)", 1);
		String returnStr = requestURLPart;
		if (matchedURL.size() > 0) {
			returnStr =  matchedURL.get(0);
			System.out.println("returnStr " + returnStr);
			List<String> matchedScrb = CommonUtil.captureMatchedStringList(returnStr, "([^\\s]+)crb=([^\\s^\\&]+)", 2);
			if (matchedScrb.size() > 0) {
				returnStr = returnStr.replaceAll(matchedScrb.get(0), "");
				System.out.println("returnStr2 " + returnStr);
			}
			
			List<String> matchedUserInfo = CommonUtil.captureMatchedStringList(returnStr, "([^\\s]+)user([^\\s^=]*)=([^\\s^\\&]+)", 3);
			if (matchedUserInfo.size() > 0) {
				System.out.println(" Found match1 " + matchedUserInfo.get(0));
				returnStr = returnStr.replaceAll(matchedUserInfo.get(0), "");
			}
			
			
			List<String> matchedCompanyInfo = CommonUtil.captureMatchedStringList(returnStr, "([^\\s]+)company([^\\s^=]*)=([^\\s^\\&]+)", 3);
			if (matchedCompanyInfo.size() > 0) {
				System.out.println(" Found match2 " + matchedCompanyInfo.get(0));
				returnStr = returnStr.replaceAll(matchedCompanyInfo.get(0), "");
				System.out.println("returnStr3 " + returnStr);
			}
		}
		
		
		
	}

}
