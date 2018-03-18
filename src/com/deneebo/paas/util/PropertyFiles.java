package com.deneebo.paas.util;


import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyFiles 
{
	Properties properties = new Properties();
	public Properties cassandra() throws IOException
	{
		InputStream in = this.getClass().getResourceAsStream("cassandra.properties");
		properties.load(in);
		return properties;
	}
	
	public Properties socket() throws IOException
	{
		
		InputStream in = this.getClass().getResourceAsStream("socket.properties");
		properties.load(in);
		return properties;
	}
	
	public Properties kafka() throws IOException
	{
		
		InputStream in = this.getClass().getResourceAsStream("kafka.properties");
		properties.load(in);
		return properties;
	}
	
	public Properties zookeeper() throws IOException
	{
		InputStream in = this.getClass().getResourceAsStream("zookeeper.properties");
		properties.load(in);
		return properties;
	}
}
