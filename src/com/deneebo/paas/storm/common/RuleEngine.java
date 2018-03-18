package com.deneebo.paas.storm.common;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

import com.deneebo.paas.storm.cassandra.CassandraConnector;


/**
 * This class is used to execute list of rule & perform corresponding action(EMAIL/SMS) 
 * @author  deneebo
 * @version 1.0, 08/12/13
 * @since   1.0
 */

public class RuleEngine 
{
	static Logger logger = Logger.getLogger("RULEBOLT");
	static Statement statement=null;
	static ResultSet resultset=null;
	/**
	 * This method used for return available rule for specific event/device
	 * @param keyspacename as String
	 * @param eventid as String
	 * @param deviceid as String
	 * @return rulelist as ArrayList
	 */
	public static ArrayList<String> getRule(String keyspacename,String eventid,String deviceid)
	{
		ArrayList<String> rulelist=new ArrayList<String>();
		try
		{
			statement=CassandraConnector.getConnection(keyspacename);
			String query="select * from rulesbyevent where key='"+eventid+"'";
			
			resultset=statement.executeQuery(query);
			int cout=resultset.getMetaData().getColumnCount();
			for(int i=2;i<=cout;i++)
			{
				rulelist.add(resultset.getString(i));
			}
			
		}
		catch(Exception e)
		{
			logger.error("Exception @ RuleEngine :getRule");
			e.printStackTrace();
		}
	return rulelist;
		
	}
	/**
	 * 
	 * @param rulelist 
	 * @param deviceid
	 * @param stream
	 * 
	 */
	public static void executeRule(ArrayList<String> rulelist,String keyspacename,String deviceid,JSONObject stream)
	{
		try
		{
			Iterator<String> iterator=rulelist.iterator();
			while (iterator.hasNext())
			{
				String ruleid = (String) iterator.next();
				statement=CassandraConnector.getConnection(keyspacename);
				String query="select * from rules where key='"+ruleid+"'";
				resultset=statement.executeQuery(query);
				while(resultset.next())
				{
					String rule_status=resultset.getString("rulestatus");
					if(rule_status.equalsIgnoreCase("Enabled"))
					{
						String tagid=resultset.getString("tagid");
						query="select tagname from tags where key='"+tagid+"'";
						ResultSet tagresultset=statement.executeQuery(query);
						String tagname=tagresultset.getString("tagname");
						String rule=resultset.getString("availrule");
						double threshold=Double.parseDouble(resultset.getString("threshold"));
						String service = resultset.getString("service");
						System.out.println("Trigger For :"+tagname +""+rule +""+threshold  +"    Action  -:"+service);
					/** Add jsonstream into linkedlist**/
						stream.keySet();
						String triggermessage=null;
						for (Object jsonobject : stream.keySet()) 
						{
							double tagvalue;
							/*if(jsonobject.equals(tagname))
							{
								System.out.println("Tag Name Matched with rule tag "+tagname+":"+stream.getString(tagname));
								System.out.println("Rule Name"+rule);
								switch(rule)
								{
								case "<": 
									tagvalue=Double.parseDouble(stream.getString(tagname));
									if(tagvalue<threshold)
									{
										triggermessage=" For this Device ID "+deviceid+"  Your "+tagname+" value is "+tagvalue+". It is less than the threhold limit";
										RuleService.executeRule(ruleid, service, triggermessage, keyspacename);
									}
									break;
								case ">":
									tagvalue=Double.parseDouble(stream.getString(tagname));
									System.out.println("tagvalue:"+tagvalue +"	Threshold value"+threshold);
									if(tagvalue>threshold)
									{
										triggermessage=" For this Device ID "+deviceid+ "  Your "+tagname+" value is "+tagvalue+". It exceeds the threhold limit";
										System.out.println(triggermessage);
										RuleService.executeRule(ruleid, service, triggermessage, keyspacename);
									}
									break;
							case "<=":
									tagvalue=Double.parseDouble(stream.getString(tagname));
									if(tagvalue<threshold)
									{
										triggermessage=" For this Device ID "+deviceid+" Your "+tagname+" value is "+tagvalue+". It is less than equals the threhold limit";
										RuleService.executeRule(ruleid, service, triggermessage, keyspacename);
									}
									break;
								case ">=":
										tagvalue=Double.parseDouble(stream.getString(tagname));
										if(tagvalue<threshold)
										{
											triggermessage= "For this Device ID "+deviceid+" Your "+tagname+" value is "+tagvalue+". It exceeds the threhold limit";
											RuleService.executeRule(ruleid, service, triggermessage, keyspacename);
										}
										break;
								case "==":
									tagvalue=Double.parseDouble(stream.getString(tagname));
									if(tagvalue<threshold)
									{
										triggermessage= "For this Device ID "+deviceid+" Your "+tagname+" value is "+tagvalue+". It exceeds the threhold limit";
										RuleService.executeRule(ruleid, service, triggermessage, keyspacename);
									}
									break;
								case "|":
								
								String[] orThresh=threshold1.split(",");
								String firstThresh=orThresh[0];
								String secondtThresh=orThresh[1];
								double firstThreshold = Double.parseDouble(firstThresh);
								double secondthresh = Double.parseDouble(secondtThresh);
															
								if(streamtagvalues == firstThreshold || streamtagvalues == secondthresh)
								{
									
									deneeboForm="Your rule is [if ("+tagName +"<="+firstThreshold +")but you data value is"+ streamtagvalues+"]";
									System.out.println("| rule Triggered for tagName:"+tagName +"  Action is :"+service);
									System.out.println(deneeboForm);
									RuleAction.executeaction(ruleid,service,deneeboForm,keyspaceName,deviceid);
									break;
									
								}
								case "==":
								threshholdValue = Double.parseDouble(threshold1);
								if(streamtagvalues == threshholdValue )
								{
									deneeboForm="Your rule is [ ("+tagName +"=="+threshholdValue +")but you data value is"+ streamtagvalues+"]";
									System.out.println("== rule Triggered for tagName:"+tagName +"  Action is :"+service);
									System.out.println(deneeboForm);
									RuleAction.executeaction(ruleid,service,deneeboForm,keyspaceName,deviceid);
									
								}
								else
								{
									//System.out.println("| Rule Executed");
								}
								break;

							default:
								break;
													
								}
							} */
							
						}
						
					}
				}
				
			}
			 
		}
		catch(Exception e)
		{
			logger.error("Exception @ RuleEngine :executeRule");
		}
	}
}
