package com.deneebo.paas.storm.common;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.deneebo.paas.storm.cassandra.CassandraConnector;

/**
* This Class to Store Event data into corresponding event columnfamily
* @author  deneebo
* @version 1.0, 25/11/13
* @since   1.0
*/

public class EventDao
{ 
	static Logger logger = Logger.getLogger("CASSANDRAERROR_APPENDER");
	/** @param cfname is String value 
	 *  @param jSstream is JSONObject 
	 * @param dynamickeyspace as String 
	 * */
	public static void insertCassandra( String dynamickeyspace,String cfname, JSONObject jSstream) throws Exception
	{
	long timestamp = 0;
	Date date = null;		
	String streamdate="";
	String deviceid="";
	String key1=(String) jSstream.get("KEY");
  	String Key[]=key1.split("-");              
  	String devicetype=Key[1];                        //Key[1] contains devicetype for to identify event type
  	deviceid = Key[2];    
  	ResultSet resultset = null;
	String cql="";
	Statement st=CassandraConnector.getConnection(dynamickeyspace);
	
	String key="";
	jSstream.keySet();
	String c=""; String v="";
	for (Object j : jSstream.keySet()) 
	    {
		   c+=j+",";
		   v+="'"+(String) jSstream.get(j)+"',";
		date=new Date();
	    timestamp=date.getTime();
	   // key=deviceid+timestamp;
	    key=deviceid;
	    			    		   
	    }
	c=c.substring(0,c.length()-1);
	v=v.substring(0,v.length()-1);
	
	/** Generate Server Receiving time**/
    String Receivedtime="";
    Format formatter;
   //Date date = new Date();
    formatter = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    Receivedtime = formatter.format(date);
    formatter = new SimpleDateFormat("yyyyMMdd");
    streamdate = formatter.format(date);
    formatter = new SimpleDateFormat("yyyy");
    String year = formatter.format(date);
    formatter = new SimpleDateFormat("MM");
    String month = formatter.format(date);
    
    try
    {
     cql="select * from devicetypes where key='"+devicetype+"'";
     Statement stmt=CassandraConnector.getConnection(dynamickeyspace);
	 resultset=stmt.executeQuery(cql);
	 }
    catch(Exception e)
    {
    	logger.error(e);
    }
   /** If Device is movable device Latitude/Longitude will update into locationbydevice columnfamily**/
    // System.out.println(rs1.getMetaData().getColumnCount());
	 if(resultset.getMetaData().getColumnCount()>6)
	 {
		 String lat=resultset.getString("Latitude");
		 String lon=resultset.getString("Longitude");
		 cql="select * from tags where key='"+lat+"'"; 
		 resultset=st.executeQuery(cql);
		 lat=resultset.getString("tagname");
		 cql="select * from tags where key='"+lon+"'"; 
		 resultset=st.executeQuery(cql);
		 lon=resultset.getString("tagname");
		 String name[]=c.split(",");
		 String value[]=v.split(",");
		 List valuelist = Arrays.asList(value);
		 List namelist = Arrays.asList(name);
		 int index=namelist.indexOf(lat);
		 String latvalue=(String) valuelist.get(index);
		 index=namelist.indexOf(lon);
		 String lonvalue=(String) valuelist.get(index);
		 cql="insert into locationsbydevice(key,latitude,longitude,status)values('"+key+"',"+latvalue+","+lonvalue+",'ON')";
		 st.executeUpdate(cql);
		 
	 }
	/** insert Event Data into Standard column family***/
	 	cql="insert into "+cfname+"(key,ReceivedTime,year,month,streamdate,"+c+")values('"+Receivedtime+"','"+Receivedtime+"','"+year+"','"+month+"','"+streamdate+"',"+v+")";
	    System.out.println(cql);
	 	st.executeUpdate(cql);
	    System.out.println("Inserted data into "+cfname +"  Columfamily");
	    
	    cfname=cfname+"bydevice";
	    cql="insert into "+cfname+"(key,'"+Receivedtime+"')values('"+key+"','"+Receivedtime+"')";
		st.executeUpdate(cql);
		System.out.println("Inserted data into "+cfname +"  Columfamily");
	}	
	
	public static void insertformdata( String dynamickeyspace,String cfname, JSONObject jSstream) throws Exception
	{
	
	Date date = null;	
	long timestamp;
	String streamdate="";
	String key1=(String) jSstream.get("KEY");
  	String Key[]=key1.split("-");              
  	String devicetype=Key[1];                        //Key[1] contains devicetype for to identify event type
   	ResultSet resultset = null;
	String cql="";	
	Statement st=CassandraConnector.getConnection(dynamickeyspace);
	String key = "";
	jSstream.keySet();
	String c=""; String v="";
	for (Object j : jSstream.keySet()) 
	    {
		   c+=j+",";
		   v+="'"+(String) jSstream.get(j)+"',";
		date=new Date();
		timestamp=date.getTime();;
	   // key=deviceid+timestamp;
	    key=cfname+timestamp;
	    			    		   
	    }
	c=c.substring(0,c.length()-1);
	v=v.substring(0,v.length()-1);
	
	/** Generate Server Receiving time**/
    String Receivedtime="";
    Format formatter;
   //Date date = new Date();
    formatter = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    Receivedtime = formatter.format(date);
    formatter = new SimpleDateFormat("yyyyMMdd");
    streamdate = formatter.format(date);
    formatter = new SimpleDateFormat("yyyy");
    String year = formatter.format(date);
    formatter = new SimpleDateFormat("MM");
    String month = formatter.format(date);
   
    /** insert Event Data into Standard column family***/
	 	cql="insert into "+cfname+"(key,ReceivedTime,year,month,streamdate,"+c+")values('"+Receivedtime+"','"+Receivedtime+"','"+year+"','"+month+"','"+streamdate+"',"+v+")";
	    st.executeUpdate(cql);
	    System.out.println("Inserted data into "+cfname +"  Columfamily");
	    cfname=cfname+"byform";
	    cql="insert into "+cfname+"(key,'"+Receivedtime+"')values('"+key+"','"+Receivedtime+"')";
		st.executeUpdate(cql);
		System.out.println("Inserted data into "+cfname +"  Columfamily");
	}	

}
