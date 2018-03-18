package com.deneebo.paas.storm.common;


import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

/**
 * This class is convert any type of Stream into JSONObject   
 * @author deneebo
 * @version 1.0, 03/12/13
 * @since   1.0
 */
public class JsonConverter
{
	/**
	 * This method use to convert ANY type of Stream into JSONObject 
	 * @param Stream as String
	 * @return jsonObject as JSONObject
	 */
	static Logger logger = Logger.getLogger("JSONCONVERTER_APPENDER");
	public static JSONObject convert(String Stream)
	{
	JSONObject jsonObject=null;
		try
		{
			String[] datatype=Stream.split("/");	
			if(datatype[0].equals("CSV"))
			{
				jsonObject=processCSV(Stream);
			}
		}
		catch(Exception e)
		{
			logger.error("Error @ JsonConverter:convert  :"+Stream ,e);
			e.printStackTrace();
		}
		return jsonObject;
	}
	
	/**
	 * this method used to convert CSV(,) data to JSONObject 
	 * @param Data as String
	 * @return jsonObject as JSONObject
	 * @throws Exception
	 */
	 private static JSONObject processCSV(String Data) throws Exception
	 {
		 JSONObject jsonObject = new JSONObject();
		 
		try
		{
		 String[] receiver; 
		 String[] rawdata;
		 receiver=Data.split("/",2);
		 String receiver1=receiver[0];
		 String delimiter =",";
		 rawdata = receiver[1].split(delimiter);
		
		
		 for(int i =0; i < rawdata.length ; i++)
		 {
		     String[] NameValue; 
			 String delimiter2 ="=";			 
			 NameValue = rawdata[i].split(delimiter2);
			 jsonObject.put(NameValue[0], NameValue[1]);
	
		  }
		 
		}
	
		catch(Exception e)
		{
			logger.error("Error@ JsonConverter:processCSV-Unable to conver CSV to JSON:"+Data, e);
			e.printStackTrace();
		}
		return jsonObject;
	 }
	 

}
