package com.pwan;

import java.util.Vector;

import org.apache.axis.client.Call;
import org.apache.axis.client.Service;


public class TestCalculate {

	public static void main(String[] args) {
		System.out.println("This is the start of test apps");
		
		try {
			   
			   String endpoint = "http://www.webxml.com.cn/WebServices/WeatherWebService.asmx?wsdl";
			   Service service = new Service();
			   Call call = service.createCall();
			   
			   // 设置service所在URL
			   call.setTargetEndpointAddress(new java.net.URL(endpoint));
			   call.setOperationName(new QName("http://WebXml.com.cn/", "getWeatherbyCityName"));
			   call.addParameter(new QName("http://WebXml.com.cn/","theCityName"),org.apache.axis.encoding.XMLType.XSD_STRING, javax.xml.rpc.ParameterMode.IN);
			   call.setUseSOAPAction(true);
			   call.setReturnType(org.apache.axis.encoding.XMLType.SOAP_VECTOR); 
			   call.setSOAPActionURI("http://WebXml.com.cn/getWeatherbyCityName");
			   Vector ret =  (Vector) call.invoke(new Object[]{"大庆"});
			   System.out.println("--------"+ret);
			  } catch (Exception e) {
			   // TODO Auto-generated catch block
			   e.printStackTrace();
			  }

	}

}
