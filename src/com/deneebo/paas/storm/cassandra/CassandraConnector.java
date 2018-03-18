package com.deneebo.paas.storm.cassandra;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.esotericsoftware.minlog.Log;
/**
 * This class is used to get statement from cassandra instance   
 * @author deneebo
 * @version 1.0, 03/12/13
 * @since   1.0
 */
public class CassandraConnector {
	
	static Properties prop = new Properties();
	static String[] nodes = null;
	static public String version = "2.0.0";
	static public Statement st = null;
	static public Connection con = null; 
	static Logger logger = Logger.getLogger("CASSANDRAERROR_APPENDER");
	
	public static Statement getConnection(String keyspace) throws Exception 
	{
		// TODO Auto-generated method stub
		try {
			Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");
		}
		catch(Exception e) {
			logger.error("Cassandra-jdbc driver not available", e);
		}
		try {
			prop.load(new FileInputStream("deneebolog4j/cassandra.properties"));
			nodes = prop.getProperty("nodes").split(",");
			
		}
		catch(Exception e) {
			e.printStackTrace();
			logger.error("File not found... or... Nodes not avaible" ,e);
		}
		
		for (String node: nodes)
	    {
			try {
				con = DriverManager.getConnection("jdbc:cassandra://"+node+"/"+keyspace+"?version="+version);
				break;
			}
			catch(Exception e)
			{
				//e.printStackTrace();
				logger.error("unable to connect Cassandra");
				
			}
	    }
		
		if(con==null) {
			logger.error("Cassandra cluster failed...... All nodes are down or Keyspace not available......");
			System.out.println("Cassandra cluster failed...... All nodes are down or Keyspace not available......");
		}
		else {
			
			st=con.createStatement();
		}
		return st;
	}
}