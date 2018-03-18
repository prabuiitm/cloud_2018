package com.deneebo.paas.storm.common;

import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.log4j.Logger;

import com.deneebo.paas.storm.cassandra.CassandraConnector;

/**
 * This class used Authenticate client information like client registered,device registered and device type registered 
 * @author 
 * @version
 */
public class Authentication 
{
	static Logger logger = Logger.getLogger("AUTHEN_APPENDER");
	static Logger cassandralogger = Logger.getLogger("CASSANDRAERROR_APPENDER");
	/**
	 * 
	 * @param clientid
	 * @return
	 */
	static Statement statement=null;
	static ResultSet resultset=null;
	static String query=null;
	public static boolean isautheroziedClient(String clientid)
	{
		boolean autherization=false;
		try
		{
			statement=CassandraConnector.getConnection("Deneebo");
			query="select * from users where key='"+clientid+"'";
			resultset=statement.executeQuery(query);
			if(resultset.getMetaData().getColumnCount()>1)
			{
				autherization=true;
			}
			
		}
		catch(Throwable e)
		{
			e.printStackTrace();
		}
		
		return autherization;
		
	}
	
	/**
	 * 
	 * @param keyspacename
	 * @param devicetypeid
	 * @return
	 */
	public static boolean isautheroziedDevicetype(String keyspacename,String devicetypeid)
	{
		boolean autherization=false;
		try
		{
			statement=CassandraConnector.getConnection(keyspacename);
			query="select * from devicetypes where key='"+devicetypeid+"'";
			resultset=statement.executeQuery(query);
			if(resultset.getMetaData().getColumnCount()>1)
			{
				autherization=true;
			}
		}
		catch(Exception e)
		{
			cassandralogger.error("Unable to connect cassandra @ Authentication :isautheroziedDevicetype");
			System.out.println("Error @ Authentication :isautheroziedDevicetype");
			e.printStackTrace();
		}
		return autherization;
		
	}
	/**
	 * 
	 * @param keyspacename
	 * @param devicetypeid
	 * @param deviceid
	 * @return
	 */
	public static boolean isautheroziedDevice(String keyspacename,String devicetypeid,String deviceid)
	{
		boolean autherization=false;
		try
		{
			statement=CassandraConnector.getConnection(keyspacename);
			query="select * from authenticatedbydevice where key='"+deviceid+"'";
			resultset=statement.executeQuery(query);
			if(resultset.getMetaData().getColumnCount()>1)
			{
				autherization=true;
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();	
		}
		return autherization;
		
	}
	/**
	 * 
	 * @param clientid
	 * @return
	 *  
	 */
	public static String getclientKeyspace(String clientid)
	{
		String keyspacename=null;
		try
		{
			statement=CassandraConnector.getConnection("Deneebo");
			query="select * from users where key='"+clientid+"'";
			resultset=statement.executeQuery(query);
			/*for(int i=1;i<=resultset.getMetaData().getColumnCount();i++)
			{
				System.out.println(resultset.getString(i));
			}*/
			keyspacename=resultset.getString("Keyspace");
						
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		return keyspacename;
		
	}
}
