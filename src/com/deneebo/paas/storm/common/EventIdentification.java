package com.deneebo.paas.storm.common;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import org.json.simple.JSONObject;

import com.deneebo.paas.storm.cassandra.CassandraConnector;

/**
 * class description
 * @author 
 * @version
 */
public class EventIdentification 
{

	/**
	 * 
	 * @param clientid
	 * @param devicetypeid
	 * @param stream
	 * @return
	 */
	static Statement statement=null;
	static ResultSet resultset=null;
	public static String getEvent(String keyspacename,String devicetypeid,JSONObject stream)
	{
		String eventid=null;
		try
		{
		//	System.out.println("keyspace name   :"+keyspacename);
			statement=CassandraConnector.getConnection(keyspacename);
			String query="select * from eventsbydevicetype where key='"+devicetypeid+"'";
			resultset=statement.executeQuery(query);
			Set<String> eventtags=new HashSet<String>();
			Set<String> streamtags=new HashSet<String>();
			stream.keySet();
			for (Object tag : stream.keySet()) 
			{
				if(!tag.equals("KEY"))
				streamtags.add((String) tag);
			}
		//	System.out.println(streamtags);
		//	System.out.println("stream size : "+streamtags.size());
			int eventcount=resultset.getMetaData().getColumnCount();
			System.out.println("Number of Event Device type :"+devicetypeid+" :"+eventcount);
			boolean evntmatch_flag=false;
			for (int i = 2; i <= eventcount; i++)
			{
				String registeredeventid=resultset.getString(i);
				System.out.println("Checking Eventid:"+registeredeventid);
				query="select * from tagsbyevent where key='"+registeredeventid+"'";
				statement=CassandraConnector.getConnection(keyspacename);
				ResultSet tagresultset=statement.executeQuery(query);
				int tagcount=tagresultset.getMetaData().getColumnCount();
				int c=tagcount-1;
		//		System.out.println("Event tagcount count:"+c);
		//		System.out.println("Stream tagcount count:"+streamtags.size());
				while(tagcount>1 && c==streamtags.size())
				{
					
					//System.out.println("Stream Tag Count Matched With Event Tag Count");
					//System.out.println(tagcount+" : "+tagresultset.getString(tagcount));
					eventtags.add(getTagname(keyspacename,tagresultset.getString(tagcount)));
					tagcount=tagcount-1;
				}
				//System.out.println("after Size : "+resultset.getMetaData().getColumnCount());
				evntmatch_flag=streamtags.equals(eventtags);
	    		System.out.println("Event Matched  -:"+evntmatch_flag);				
				if(evntmatch_flag) 
				{
					evntmatch_flag=true;
					eventid=registeredeventid;
					break;
				}
			}
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return eventid;
		
	}
	public static String getTagname(String keyspacename,String tagid) 
	{
		String tagname=null;
		try
		{
			statement=CassandraConnector.getConnection(keyspacename);
			String query="select * from tags where key='"+tagid+"'";
			ResultSet tagnameresultset=statement.executeQuery(query);
			tagname=tagnameresultset.getString("tagname");
			//System.out.println(tagname);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		return tagname;
	}
}
