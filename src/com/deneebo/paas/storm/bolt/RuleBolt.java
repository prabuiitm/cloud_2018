package com.deneebo.paas.storm.bolt;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.deneebo.paas.storm.common.RuleEngine;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class RuleBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	/**
	 * Logger object for the AuthenticationBolt class
	 */
	static Logger logger = Logger.getLogger("RULEBOLT");
	OutputCollector _OutputCollector;
	String keyspacename=null;
	String eventid=null;
	String deviceid=null;
	String stream=null;
	Statement statement=null;
	JSONObject jsonstreamobject;
	ArrayList<String> rulelist=new ArrayList<String>();
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		_OutputCollector=collector;

	}
static int count=0;
	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		count=count+1;
		System.out.println("*******************************************");
		System.out.println("IN Rule Bolt Count::"+count);
		System.out.println("*******************************************");
		System.out.println("Tuple inside of RuleBolt Bolt   -:"+input.getValue(0).toString());
		String[] uniqueidentifier=input.getString(0).split(":",4);
		keyspacename=uniqueidentifier[0];//keyspace Name
		eventid=uniqueidentifier[1];     //Event Name
		deviceid=uniqueidentifier[2];      
		stream=uniqueidentifier[3]; // Stream
		JSONParser parser = new JSONParser();

        Object obj=null;
		try {
			obj = parser.parse(stream);
		} catch (ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		jsonstreamobject=(JSONObject)obj; //convert string to JsonObject
		try
		{
			rulelist=RuleEngine.getRule(keyspacename, eventid, deviceid);
			System.out.println("Number of Rule for this event   -:"+rulelist.size());
			if(!rulelist.isEmpty()) { RuleEngine.executeRule(rulelist,keyspacename, deviceid, jsonstreamobject); }
			/** To Cassandra write Bolt**/
			String input_string=keyspacename+":"+eventid+":"+stream;
			_OutputCollector.emit(new Values(input_string));
			_OutputCollector.ack(input);
		}
		catch(Exception e)
		{
			e.printStackTrace();
			logger.error("Exception @ Rule Bolt:execute  :"+e);
		}

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		 declarer.declare(new Fields("cassandrastream"));

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
